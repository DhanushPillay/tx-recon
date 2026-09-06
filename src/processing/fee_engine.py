import os
from dataclasses import dataclass

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


class FeeEngine:
    def __init__(self, config_path: str | None = None):
        settings = get_settings()
        path = config_path or os.path.join(settings.project_root, settings.fee_rate_config)

        self.config_version = "v1"
        if os.path.exists(path):
            with open(path) as f:
                self.config = yaml.safe_load(f)
        else:
            self.config = {
                "version": "v1.0.0",
                "default": {"mdr_rate_bps": 150, "gst_on_mdr": 18.0, "tolerance_paise": 1},
                "instruments": {},
            }

        self.config_version = self.config.get("version", "unknown")

    @property
    def default_rate(self) -> dict:
        return self.config.get("default", {})

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

    def get_rate(self, instrument_type: str, merchant_id: str | None = None) -> dict:
        merchants = self.config.get("merchants", {})
        if merchant_id and merchant_id in merchants:
            merchant_rates = merchants[merchant_id]
            if instrument_type in merchant_rates:
                return {**self.default_rate, **merchant_rates[instrument_type]}

        instruments = self.config.get("instruments", {})
        if instrument_type in instruments:
            return {**self.default_rate, **instruments[instrument_type]}

        return self.default_rate

    def compute_fee(
        self, amount_paise: int, instrument_type: str = "UPI", merchant_id: str | None = None
    ) -> FeeResult:
        """Fee + GST in integer paise. Raises ValueError on negative amounts."""
        if amount_paise < 0:
            raise ValueError(f"amount_paise must be non-negative, got {amount_paise}")
        rate = self.get_rate(instrument_type, merchant_id)
        mdr_bps = rate.get("mdr_rate_bps", 150)
        gst_pct = rate.get("gst_on_mdr", 0)

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
            rate_version=self.config_version,
        )

    def compute_expected_settled(
        self, amount_paise: int, instrument_type: str = "UPI", merchant_id: str | None = None
    ) -> int:
        return self.compute_fee(amount_paise, instrument_type, merchant_id).net_paise

    def check_match(
        self,
        amount_paise: int,
        settled_amount_paise: int,
        instrument_type: str = "UPI",
        merchant_id: str | None = None,
    ) -> tuple[bool, FeeResult]:
        rate = self.get_rate(instrument_type, merchant_id)
        tolerance = rate.get("tolerance_paise", 1)

        result = self.compute_fee(amount_paise, instrument_type, merchant_id)
        diff = abs(result.net_paise - settled_amount_paise)
        return diff <= tolerance, result


_fee_engine: FeeEngine | None = None


def get_fee_engine() -> FeeEngine:
    global _fee_engine
    if _fee_engine is None:
        _fee_engine = FeeEngine()
    return _fee_engine
