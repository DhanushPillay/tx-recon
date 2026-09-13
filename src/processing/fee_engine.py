import os
from dataclasses import dataclass
from datetime import date

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


class FeeEngine:
    def __init__(self, config_path: str | None = None):
        settings = get_settings()
        path = config_path or os.path.join(settings.project_root, settings.fee_rate_config)

        self.config_version = "v1"
        if os.path.exists(path):
            with open(path) as f:
                loaded = yaml.safe_load(f) or {}
        else:
            loaded = {}
        if not loaded:
            loaded = {
                "version": "v1.0.0",
                "default": {"mdr_rate_bps": 150, "gst_on_mdr": 18.0, "tolerance_paise": 1},
                "instruments": {},
            }
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
            if float(default["gst_on_mdr"]) < 0:
                raise ValueError(f"invalid {label} default gst_on_mdr: {default['gst_on_mdr']!r}")
            if int(default["tolerance_paise"]) < 0:
                raise ValueError(
                    f"invalid {label} default tolerance_paise: {default['tolerance_paise']!r}"
                )
            for inst, rate in (card.get("instruments", {}) or {}).items():
                bps = int(rate.get("mdr_rate_bps", default["mdr_rate_bps"]))
                if not (0 <= bps <= 10000):
                    raise ValueError(f"invalid mdr_rate_bps for {inst} in {label}: {bps!r}")
            for merch, inst_map in (card.get("merchants", {}) or {}).items():
                if not isinstance(inst_map, dict):
                    raise ValueError(f"invalid merchants.{merch} in {label}: must be dict")
                for inst, rate in inst_map.items():
                    bps = int(rate.get("mdr_rate_bps", default["mdr_rate_bps"]))
                    if not (0 <= bps <= 10000):
                        raise ValueError(
                            f"invalid mdr_rate_bps for merchants.{merch}.{inst} in {label}: {bps!r}"
                        )

        history = self.config.get("history") or self.config.get("rate_cards")
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
        else:
            validate_card(self.config, "default")

    def _build_rate_cards(self) -> None:
        history = self.config.get("history") or self.config.get("rate_cards")
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
        # Find latest card where effective_from <= d and (effective_to is None or d <= effective_to)
        chosen = self.rate_cards[0]
        for card in self.rate_cards:
            eff_from = _parse_iso_date(str(card.get("effective_from", "1970-01-01")))
            eff_to = (
                _parse_iso_date(str(card["effective_to"])) if card.get("effective_to") else None
            )
            if eff_from and d >= eff_from and (eff_to is None or d <= eff_to):
                chosen = card
            elif eff_from and d >= eff_from and eff_to is None:
                # No effective_to — ongoing
                chosen = card
        # If date before all cards, use earliest
        earliest = _parse_iso_date(str(self.rate_cards[0].get("effective_from", "1970-01-01")))
        if earliest and d < earliest:
            return self.rate_cards[0]
        return chosen

    def get_rate(self, instrument_type: str, merchant_id: str | None = None) -> dict:
        card = self.rate_cards[-1]
        merchants = card.get("merchants", {}) or {}
        # Fallback to top-level merchants for tests that mutate config directly
        if merchant_id and merchant_id not in merchants:
            merchants = {**merchants, **(self.config.get("merchants", {}) or {})}
        if merchant_id and merchant_id in merchants:
            merchant_rates = merchants[merchant_id]
            if instrument_type in merchant_rates:
                return {**card.get("default", {}), **merchant_rates[instrument_type]}

        instruments = card.get("instruments", {})
        if instrument_type in instruments:
            return {**card.get("default", {}), **instruments[instrument_type]}

        return card.get("default", {})

    def get_rate_for_date(
        self,
        instrument_type: str,
        merchant_id: str | None = None,
        settlement_date: str | None = None,
    ) -> dict:
        card = self._card_for_date(settlement_date)
        merchants = card.get("merchants", {}) or {}
        if merchant_id and merchant_id not in merchants:
            merchants = {**merchants, **(self.config.get("merchants", {}) or {})}
        if merchant_id and merchant_id in merchants:
            merchant_rates = merchants[merchant_id]
            if instrument_type in merchant_rates:
                return {**card.get("default", {}), **merchant_rates[instrument_type]}
        instruments = card.get("instruments", {})
        if instrument_type in instruments:
            return {**card.get("default", {}), **instruments[instrument_type]}
        return card.get("default", {})

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
        gst_bps = int(round(float(gst_pct) * 100))
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
