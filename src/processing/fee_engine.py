import os
from dataclasses import dataclass
from typing import Optional

import yaml

from src.common.settings import get_settings


@dataclass
class FeeResult:
    fee_paise: int
    net_paise: int
    rate_bps: int
    gst_paise: int
    instrument_type: str


class FeeEngine:
    def __init__(self, config_path: Optional[str] = None):
        settings = get_settings()
        path = config_path or os.path.join(settings.project_root, settings.fee_rate_config)
        if os.path.exists(path):
            with open(path) as f:
                self._config = yaml.safe_load(f)
        else:
            self._config = {
                "default": {"mdr_rate_bps": 150, "gst_on_mdr": 18.0, "tolerance_paise": 1},
                "instruments": {},
            }

        self._default = self._config.get("default", {})
        self._instruments = self._config.get("instruments", {})
        self._merchants = self._config.get("merchants", {})

    def get_rate(self, instrument_type: str, merchant_id: Optional[str] = None) -> dict:
        if merchant_id and merchant_id in self._merchants:
            merchant_rates = self._merchants[merchant_id]
            if instrument_type in merchant_rates:
                return {**self._default, **merchant_rates[instrument_type]}

        if instrument_type in self._instruments:
            return {**self._default, **self._instruments[instrument_type]}

        return self._default

    def compute_fee(
        self, amount_paise: int, instrument_type: str = "UPI", merchant_id: Optional[str] = None
    ) -> FeeResult:
        rate = self.get_rate(instrument_type, merchant_id)
        mdr_bps = rate.get("mdr_rate_bps", 150)
        gst_pct = rate.get("gst_on_mdr", 0)

        fee_before_gst = (amount_paise * mdr_bps) // 10000
        gst = int(fee_before_gst * gst_pct / 100) if gst_pct > 0 else 0
        total_fee = fee_before_gst + gst
        net = amount_paise - total_fee

        return FeeResult(
            fee_paise=total_fee,
            net_paise=net,
            rate_bps=mdr_bps,
            gst_paise=gst,
            instrument_type=instrument_type,
        )

    def compute_expected_settled(
        self, amount_paise: int, instrument_type: str = "UPI", merchant_id: Optional[str] = None
    ) -> int:
        return self.compute_fee(amount_paise, instrument_type, merchant_id).net_paise

    def check_match(
        self,
        amount_paise: int,
        settled_amount_paise: int,
        instrument_type: str = "UPI",
        merchant_id: Optional[str] = None,
    ) -> tuple[bool, FeeResult]:
        rate = self.get_rate(instrument_type, merchant_id)
        tolerance = rate.get("tolerance_paise", 1)

        result = self.compute_fee(amount_paise, instrument_type, merchant_id)
        diff = abs(result.net_paise - settled_amount_paise)
        return diff <= tolerance, result


_fee_engine: Optional[FeeEngine] = None


def get_fee_engine() -> FeeEngine:
    global _fee_engine
    if _fee_engine is None:
        _fee_engine = FeeEngine()
    return _fee_engine
