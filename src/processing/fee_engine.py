import os
from dataclasses import dataclass
from datetime import date
from decimal import ROUND_HALF_UP, Decimal

import yaml

from src.common.settings import get_settings


@dataclass
class FeeResult:
    fee_paise: int
    net_paise: int
    rate_bps: int
    gst_paise: int
    instrument_type: str
    rate_version: str


def _parse_iso_date(s: str) -> date | None:
    try:
        return date.fromisoformat(s)
    except Exception:
        return None


_HISTORY_KEYS = ("history", "rate_card_history", "rate_cards")


def _history_of(config: dict):
    for key in _HISTORY_KEYS:
        if config.get(key):
            return config[key]
    return None


def _check_int(v, lo: int, hi: int | None, name: str, label: str) -> int:
    v = int(v)
    if v < lo or (hi is not None and v > hi):
        raise ValueError(f"invalid {name} {label}: {v!r}")
    return v


def _check_bps(bps, label: str) -> int:
    return _check_int(bps, 0, 10000, "mdr_rate_bps", label)


def _check_tol(tol, label: str) -> int:
    return _check_int(tol, 0, None, "tolerance_paise", label)


def _check_gst(gst, label: str) -> float:
    """GST percent must be numeric and non-negative (mis-prices every row)."""
    try:
        v = float(gst)
    except (TypeError, ValueError):
        raise ValueError(f"invalid gst_on_mdr {label}: {gst!r}") from None
    if v < 0:
        raise ValueError(f"invalid gst_on_mdr {label}: {gst!r}")
    return v


def gst_pct_to_bps(gst_pct: float | int | str) -> int:
    """GST percent -> basis points, HALF_UP (bank_statement.py convention).

    Scalar (per rate, not per row): exact Decimal, no float. Python's
    round() is half-even and float(x)*100 carries binary error, either of
    which can shift a bps literal by 1 and misprice every row on that rate.
    """
    return int((Decimal(str(gst_pct)) * 100).to_integral_value(rounding=ROUND_HALF_UP))


class FeeEngine:
    def __init__(self, config_path: str | None = None):
        settings = get_settings()
        path = config_path or os.path.join(settings.project_root, settings.fee_rate_config)

        self.config_version = "v1"
        if not os.path.exists(path):
            # Fail closed: a missing rate card must never silently price money
            # at hardcoded defaults. Point the operator at the real file.
            raise FileNotFoundError(
                f"fee rate config not found: {path} (set via fee_rate_config / FEE_RATE_CONFIG)"
            )
        with open(path) as f:
            loaded = yaml.safe_load(f) or {}
        if not loaded:
            raise ValueError(f"fee rate config is empty: {path}")
        self.config = loaded
        self._validate_config()
        self._build_rate_cards()
        # current is last by effective_from
        self.config_version = self.rate_cards[-1].get("version", "unknown")

    def _validate_config(self) -> None:
        # Validate top-level or history entries share same shape
        def validate_card(card: dict, label: str) -> None:
            default = card.get("default", {})
            for key in ("mdr_rate_bps", "gst_on_mdr", "tolerance_paise"):
                if key not in default:
                    raise ValueError(f"fee config missing {label}.default.{key}")
            if not (0 <= int(default["mdr_rate_bps"]) <= 10000):
                raise ValueError(
                    f"invalid {label} default mdr_rate_bps: {default['mdr_rate_bps']!r}"
                )
            _check_gst(default["gst_on_mdr"], f"{label} default")
            if int(default["tolerance_paise"]) < 0:
                raise ValueError(
                    f"invalid {label} default tolerance_paise: {default['tolerance_paise']!r}"
                )
            for inst, rate in (card.get("instruments", {}) or {}).items():
                _check_bps(
                    rate.get("mdr_rate_bps", default["mdr_rate_bps"]), f"for {inst} in {label}"
                )
                _check_gst(rate.get("gst_on_mdr", default["gst_on_mdr"]), f"for {inst} in {label}")
                _check_tol(
                    rate.get("tolerance_paise", default["tolerance_paise"]),
                    f"for {inst} in {label}",
                )
            for merch, inst_map in (card.get("merchants", {}) or {}).items():
                if not isinstance(inst_map, dict):
                    raise ValueError(f"invalid merchants.{merch} in {label}: must be dict")
                for inst, rate in inst_map.items():
                    _check_bps(
                        rate.get("mdr_rate_bps", default["mdr_rate_bps"]),
                        f"for merchants.{merch}.{inst} in {label}",
                    )
                    _check_gst(
                        rate.get("gst_on_mdr", default["gst_on_mdr"]),
                        f"for merchants.{merch}.{inst} in {label}",
                    )
                    _check_tol(
                        rate.get("tolerance_paise", default["tolerance_paise"]),
                        f"for merchants.{merch}.{inst} in {label}",
                    )

        history = _history_of(self.config)
        if history:
            if not isinstance(history, list) or not history:
                raise ValueError("fee config history must be a non-empty list")
            for idx, card in enumerate(history):
                if "version" not in card or "effective_from" not in card:
                    raise ValueError(f"history[{idx}] missing version or effective_from")
                if _parse_iso_date(str(card["effective_from"])) is None:
                    raise ValueError(
                        f"history[{idx}] invalid effective_from: {card['effective_from']!r}"
                    )
                if card.get("effective_to") and _parse_iso_date(str(card["effective_to"])) is None:
                    raise ValueError(
                        f"history[{idx}] invalid effective_to: {card['effective_to']!r}"
                    )
                validate_card(card, f"history[{idx}]")
            # Overlapping ranges would price the same date two ways (Python
            # picks latest-containing, SQL picks latest-from); reject them so
            # the engines cannot diverge.
            ordered = sorted(
                enumerate(history),
                key=lambda t: (
                    _parse_iso_date(str(t[1].get("effective_from", "1970-01-01"))) or date.min
                ),
            )
            for (prev_idx, prev), (next_idx, nxt) in zip(ordered, ordered[1:], strict=False):
                prev_to = (
                    _parse_iso_date(str(prev["effective_to"])) if prev.get("effective_to") else None
                )
                next_from = _parse_iso_date(str(nxt.get("effective_from", "1970-01-01")))
                if prev_to is not None and next_from is not None and next_from <= prev_to:
                    raise ValueError(
                        f"history[{prev_idx}] and history[{next_idx}] overlap: "
                        f"{nxt.get('effective_from')!r} <= {prev.get('effective_to')!r}"
                    )
        else:
            validate_card(self.config, "default")

    def _build_rate_cards(self) -> None:
        history = _history_of(self.config)
        if history:
            cards = []
            for card in history:
                # Normalize: ensure default/instruments/merchants present
                c = dict(card)
                c.setdefault("default", self.config.get("default", {}))
                c.setdefault("instruments", self.config.get("instruments", {}))
                c.setdefault("merchants", self.config.get("merchants", {}))
                cards.append(c)
            # Sort by effective_from ascending
            cards.sort(key=lambda c: _parse_iso_date(str(c["effective_from"])) or date.min)
            self.rate_cards = cards
        else:
            # Single card from top-level, effective from 1970
            self.rate_cards = [
                {
                    "version": self.config.get("version", "v1.0.0"),
                    "effective_from": "1970-01-01",
                    "default": self.config.get("default", {}),
                    "instruments": self.config.get("instruments", {}),
                    "merchants": self.config.get("merchants", {}),
                }
            ]

    @property
    def default_rate(self) -> dict:
        return self.rate_cards[-1].get("default", {})

    def get_default_rate(self) -> dict:
        """Public accessor for the default rate card (keeps SQL builder decoupled)."""
        return dict(self.default_rate)

    @property
    def default_tolerance_paise(self) -> int:
        """Rounding tolerance used by MERGE matching (single source of truth)."""
        return int(self.default_rate.get("tolerance_paise", 1))

    @property
    def default_mdr_bps(self) -> int:
        return self.default_rate.get("mdr_rate_bps", 150)

    @property
    def default_gst_pct(self) -> float:
        return float(self.default_rate.get("gst_on_mdr", 18))

    def _card_for_date(self, settlement_date: str | None) -> dict:
        if not settlement_date:
            return self.rate_cards[-1]
        d = _parse_iso_date(str(settlement_date))
        if d is None:
            return self.rate_cards[-1]
        # Latest card whose [effective_from, effective_to] contains d;
        # gap dates fall back to last-prior card (max from <= d), pre-first -> earliest.
        chosen = prior = None
        for card in self.rate_cards:
            eff_from = _parse_iso_date(str(card.get("effective_from", "1970-01-01")))
            if not eff_from or eff_from > d:
                continue
            prior = card
            eff_to = (
                _parse_iso_date(str(card["effective_to"])) if card.get("effective_to") else None
            )
            if eff_to is None or d <= eff_to:
                chosen = card
        return chosen or prior or self.rate_cards[0]

    @staticmethod
    def _lookup(
        card: dict, instrument_type: str, merchant_id: str | None, top_merchants: dict
    ) -> dict:
        """Merchant -> instrument -> default rate lookup on one card.

        Card-level merchants win per merchant key over top-level (same merge
        as reconcile._versioned_wrap builds for SQL: {**top, **card}).
        """
        merchants = {**top_merchants, **(card.get("merchants", {}) or {})}
        if merchant_id and merchant_id in merchants:
            merchant_rates = merchants[merchant_id]
            if instrument_type in merchant_rates:
                return {**card.get("default", {}), **merchant_rates[instrument_type]}

        instruments = card.get("instruments", {})
        if instrument_type in instruments:
            return {**card.get("default", {}), **instruments[instrument_type]}
        return card.get("default", {})

    def get_rate(self, instrument_type: str, merchant_id: str | None = None) -> dict:
        card = self.rate_cards[-1]
        # Top-level merchants apply to all cards unless a card overrides them
        # (matches _build_rate_cards setdefault inheritance + SQL builder merge).
        return self._lookup(
            card, instrument_type, merchant_id, self.config.get("merchants", {}) or {}
        )

    def get_rate_for_date(
        self,
        instrument_type: str,
        merchant_id: str | None = None,
        settlement_date: str | None = None,
    ) -> dict:
        card = self._card_for_date(settlement_date)
        return self._lookup(
            card, instrument_type, merchant_id, self.config.get("merchants", {}) or {}
        )

    def compute_fee(
        self,
        amount_paise: int,
        instrument_type: str = "UPI",
        merchant_id: str | None = None,
        settlement_date: str | None = None,
    ) -> FeeResult:
        """Fee + GST in integer paise. Raises ValueError on negative amounts."""
        if amount_paise < 0:
            raise ValueError(f"amount_paise must be non-negative, got {amount_paise}")
        if settlement_date:
            rate = self.get_rate_for_date(instrument_type, merchant_id, settlement_date)
            card = self._card_for_date(settlement_date)
            version = card.get("version", self.config_version)
        else:
            rate = self.get_rate(instrument_type, merchant_id)
            version = self.config_version
        mdr_bps = rate.get("mdr_rate_bps", 150)
        gst_pct = rate.get("gst_on_mdr", 18.0)

        # Standard round-to-nearest integer algorithm (half-up equivalent for positive integers).
        # Integer-only: no float money math. GST via bps keeps SQL/Python identical.
        fee_before_gst = (amount_paise * mdr_bps + 5000) // 10000
        gst_bps = gst_pct_to_bps(gst_pct)
        gst = (fee_before_gst * gst_bps + 5000) // 10000 if gst_bps > 0 else 0
        total_fee = fee_before_gst + gst
        net = amount_paise - total_fee

        return FeeResult(
            fee_paise=total_fee,
            net_paise=net,
            rate_bps=mdr_bps,
            gst_paise=gst,
            instrument_type=instrument_type,
            rate_version=version,
        )

    def compute_expected_settled(
        self,
        amount_paise: int,
        instrument_type: str = "UPI",
        merchant_id: str | None = None,
        settlement_date: str | None = None,
    ) -> int:
        return self.compute_fee(
            amount_paise, instrument_type, merchant_id, settlement_date
        ).net_paise

    def check_match(
        self,
        amount_paise: int,
        settled_amount_paise: int,
        instrument_type: str = "UPI",
        merchant_id: str | None = None,
        settlement_date: str | None = None,
    ) -> tuple[bool, FeeResult]:
        if settlement_date:
            rate = self.get_rate_for_date(instrument_type, merchant_id, settlement_date)
        else:
            rate = self.get_rate(instrument_type, merchant_id)
        tolerance = rate.get("tolerance_paise", 1)

        result = self.compute_fee(amount_paise, instrument_type, merchant_id, settlement_date)
        diff = abs(result.net_paise - settled_amount_paise)
        return diff <= tolerance, result


_fee_engine: FeeEngine | None = None


def get_fee_engine() -> FeeEngine:
    global _fee_engine
    if _fee_engine is None:
        _fee_engine = FeeEngine()
    return _fee_engine
