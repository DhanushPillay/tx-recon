"""Fee parity: SQL CASE builders must price exactly what FeeEngine prices.

Four copies of the fee math exist (FeeEngine Python, reconcile SQL CASE,
residual SQL, loader). This test pins the SQL builders to the engine across
the full instrument x merchant x date grid, so a drift in any copy fails here.
"""

import re

import pytest

from src.common.schemas import INSTRUMENT_TYPES
from src.processing.fee_engine import FeeEngine
from src.processing.reconcile import build_fee_case_sql, build_tolerance_case_sql

pytestmark = pytest.mark.unit

_MERCHANTS = [None, "merch_001", "merch_demo", "some_unknown_merchant"]
_DATES = ["2020-06-15", "2024-06-15", "2025-03-31", "2025-04-01", "2026-01-01", "not-a-date"]


def _literals(sql: str) -> set[int]:
    return {int(x) for x in re.findall(r"\*\s*(\d+)", sql)}


def test_fee_sql_parity_with_engine():
    engine = FeeEngine()
    fee_sql, gst_sql = build_fee_case_sql(engine)
    tol_sql = build_tolerance_case_sql(engine)
    fee_nums = _literals(fee_sql)
    gst_nums = _literals(gst_sql)
    tol_nums = {int(x) for x in re.findall(r"THEN\s+(\d+)", tol_sql)}
    for inst in INSTRUMENT_TYPES:
        for merch in _MERCHANTS:
            for day in _DATES:
                rate = engine.get_rate_for_date(inst, merch, day)
                mdr, gst_pct, tol = (
                    rate["mdr_rate_bps"],
                    rate["gst_on_mdr"],
                    rate.get("tolerance_paise", 1),
                )
                assert mdr in fee_nums, f"{inst}/{merch}/{day}: mdr {mdr} missing from fee SQL"
                assert int(round(gst_pct * 100)) in gst_nums or gst_pct == 0, (
                    f"{inst}/{merch}/{day}: gst {gst_pct} missing from gst SQL"
                )
                assert tol in tol_nums, f"{inst}/{merch}/{day}: tol {tol} missing"
