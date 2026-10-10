from datetime import date, timedelta

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from src.common.schemas import INSTRUMENT_TYPES
from src.processing.fee_engine import FeeEngine
from src.processing.reconcile import build_fee_case_sql, build_tolerance_case_sql

pytestmark = pytest.mark.unit

_GRID_AMOUNTS = [1, 999, 100000, 100001, 12345789]
_GRID_DATES = [None, "", "not-a-date", "2024/01/01", "2024-01-01", "2025-04-01", "2026-06-15"]
_GRID_MERCHANTS = [None, "", "UNKNOWN", "merch_001", "merch_demo", "unlisted_merchant"]

_LADDER = ["2024-01-01", "2024-07-01", "2025-01-01", "2025-04-01", "2025-07-01", "2026-01-01"]


def _write_config(tmp_path, name: str, body: dict) -> str:
    import yaml

    path = tmp_path / name
    path.write_text(yaml.safe_dump(body))
    return str(path)


def _rate(default: dict) -> dict:
    return st.fixed_dictionaries(
        {
            "mdr_rate_bps": st.integers(min_value=0, max_value=10000),
            "gst_on_mdr": st.one_of(
                st.just(0), st.just(0.0), st.floats(min_value=0, max_value=100, allow_nan=False)
            ),
            "tolerance_paise": st.integers(min_value=0, max_value=100),
        }
    )


def _card(default: dict, merchants: list[str]) -> dict:
    insts = st.dictionaries(
        st.sampled_from(INSTRUMENT_TYPES),
        _rate(default),
        max_size=len(INSTRUMENT_TYPES),
    )
    mer = st.dictionaries(
        st.sampled_from(merchants),
        st.dictionaries(st.sampled_from(INSTRUMENT_TYPES), _rate(default), max_size=3),
        max_size=3,
    )
    return st.fixed_dictionaries(
        {
            "default": st.just(default),
            "instruments": insts,
            "merchants": mer,
        }
    )


def _config_strategy() -> st.SearchStrategy[dict]:
    default = st.fixed_dictionaries(
        {
            "mdr_rate_bps": st.integers(min_value=0, max_value=10000),
            "gst_on_mdr": st.one_of(
                st.just(0), st.just(0.0), st.floats(min_value=0, max_value=100, allow_nan=False)
            ),
            "tolerance_paise": st.integers(min_value=0, max_value=100),
        }
    )
    merchant_names = st.lists(
        st.sampled_from(["merch_001", "merch_demo", "merch_new"]),
        min_size=1,
        max_size=3,
        unique=True,
    )

    @st.composite
    def _build(draw):
        base = draw(default)
        names = draw(merchant_names)
        top_merchants = draw(
            st.dictionaries(
                st.sampled_from(names),
                st.dictionaries(st.sampled_from(INSTRUMENT_TYPES), _rate(base), max_size=3),
                max_size=3,
            )
        )
        top_instruments = draw(
            st.dictionaries(
                st.sampled_from(INSTRUMENT_TYPES), _rate(base), max_size=len(INSTRUMENT_TYPES)
            )
        )
        n_cards = draw(st.integers(min_value=0, max_value=3))
        cards = []
        for i in range(n_cards):
            card = draw(_card(base, names))
            card["version"] = f"v{i}"
            card["effective_from"] = _LADDER[i]
            card["effective_to"] = (
                date.fromisoformat(_LADDER[i + 1]) - timedelta(days=1)
            ).isoformat()
            cards.append(card)
        cfg = {
            "version": "v0.0.0",
            "default": base,
            "instruments": top_instruments,
            "merchants": top_merchants,
        }
        if cards:
            cfg["history"] = cards
        return cfg

    return _build()


def _parity(engine: FeeEngine, spark) -> None:
    import pandas as pd

    rows = [
        (amt, inst, merch, day)
        for amt in _GRID_AMOUNTS
        for inst in INSTRUMENT_TYPES
        for merch in _GRID_MERCHANTS
        for day in _GRID_DATES
    ]
    df = spark.createDataFrame(
        pd.DataFrame(rows, columns=["a", "i", "m", "d"]),
        "a bigint, i string, m string, d string",
    )
    fee_sql, gst_sql = build_fee_case_sql(
        engine, amount_col="a", inst_col="i", merchant_col="m", settlement_date_col="d"
    )
    tol_sql = build_tolerance_case_sql(
        engine, inst_col="i", merchant_col="m", settlement_date_col="d"
    )

    out = df.selectExpr(
        "a", "i", "m", "d", f"{fee_sql} AS s_fee", f"{gst_sql} AS s_gst", f"{tol_sql} AS s_tol"
    )
    mismatches = []
    for r in out.collect():
        result = engine.compute_fee(r.a, r.i, r.m, r.d)
        rate = engine.get_rate_for_date(r.i, r.m, r.d)
        want_fee_before = (r.a * result.rate_bps + 5000) // 10000
        want_gst = result.gst_paise
        want_tol = rate.get("tolerance_paise", 1)
        key = f"amt={r.a} inst={r.i} merch={r.m!r} date={r.d!r}"
        if r.s_fee != want_fee_before:
            mismatches.append(f"{key}: SQL fee_before_gst {r.s_fee} != engine {want_fee_before}")
        if r.s_gst != want_gst:
            mismatches.append(f"{key}: SQL gst {r.s_gst} != engine {want_gst}")
        if r.s_tol != want_tol:
            mismatches.append(f"{key}: SQL tolerance {r.s_tol} != engine {want_tol}")
        if r.a - r.s_fee - r.s_gst != result.net_paise:
            mismatches.append(
                f"{key}: SQL net {r.a - r.s_fee - r.s_gst} != engine {result.net_paise}"
            )
    assert not mismatches, (
        f"{len(mismatches)} SQL/Python fee divergences, first 5:\n  " + "\n  ".join(mismatches[:5])
    )


@settings(
    max_examples=25, deadline=None, suppress_health_check=[HealthCheck.function_scoped_fixture]
)
@given(config=_config_strategy())
def test_sql_matches_engine_over_synthetic_rate_cards(tmp_path_factory, spark, config):
    path = _write_config(tmp_path_factory.mktemp("cfg"), "rates.yaml", config)
    _parity(FeeEngine(config_path=path), spark)


def test_sql_matches_engine_on_shipped_config(spark):
    _parity(FeeEngine(), spark)


@pytest.mark.parametrize(
    "body,amount,inst,merch,day",
    [
        (
            {
                "default": {"mdr_rate_bps": 150, "gst_on_mdr": 18.0, "tolerance_paise": 1},
                "history": [
                    {
                        "version": "old",
                        "effective_from": "2024-01-01",
                        "effective_to": "2025-03-31",
                        "default": {"mdr_rate_bps": 700, "gst_on_mdr": 0.0, "tolerance_paise": 3},
                    },
                    {
                        "version": "new",
                        "effective_from": "2025-04-01",
                        "default": {"mdr_rate_bps": 100, "gst_on_mdr": 18.0, "tolerance_paise": 5},
                    },
                ],
            },
            100000,
            "UPI",
            None,
            "2024/01/01",
        ),
        (
            {
                "default": {"mdr_rate_bps": 150, "gst_on_mdr": 18.0, "tolerance_paise": 1},
                "instruments": {"CREDIT_CARD": {"mdr_rate_bps": 200, "gst_on_mdr": 18.0}},
                "merchants": {"m": {"CREDIT_CARD": {"mdr_rate_bps": 111, "gst_on_mdr": 18.0}}},
                "history": [
                    {
                        "version": "only",
                        "effective_from": "2024-01-01",
                        "default": {"mdr_rate_bps": 150, "gst_on_mdr": 18.0, "tolerance_paise": 1},
                        "instruments": {"CREDIT_CARD": {"mdr_rate_bps": 200, "gst_on_mdr": 18.0}},
                        "merchants": {"other": {"UPI": {"mdr_rate_bps": 7, "gst_on_mdr": 0.0}}},
                    }
                ],
            },
            100000,
            "CREDIT_CARD",
            "m",
            "2025-06-15",
        ),
        (
            {
                "default": {"mdr_rate_bps": 150, "gst_on_mdr": 18.0, "tolerance_paise": 1},
                "instruments": {"DEBIT_CARD": {"mdr_rate_bps": 100, "gst_on_mdr": 18.0}},
                "merchants": {
                    "m": {
                        "CREDIT_CARD": {"mdr_rate_bps": 180, "gst_on_mdr": 18.0},
                        "DEBIT_CARD": {"mdr_rate_bps": 50, "gst_on_mdr": 18.0},
                    }
                },
                "history": [
                    {
                        "version": "only",
                        "effective_from": "2024-01-01",
                        "default": {"mdr_rate_bps": 150, "gst_on_mdr": 18.0, "tolerance_paise": 1},
                        "instruments": {"DEBIT_CARD": {"mdr_rate_bps": 100, "gst_on_mdr": 18.0}},
                        "merchants": {
                            "m": {"CREDIT_CARD": {"mdr_rate_bps": 190, "gst_on_mdr": 18.0}}
                        },
                    }
                ],
            },
            100000,
            "DEBIT_CARD",
            "m",
            "2025-06-15",
        ),
    ],
    ids=[
        "H2-malformed-date-routes-latest",
        "H4-single-card-keeps-top-merchants",
        "H5-per-instrument-merge",
    ],
)
def test_regression_configs_stay_in_parity(tmp_path, spark, body, amount, inst, merch, day):
    path = _write_config(tmp_path, "rates.yaml", body)
    engine = FeeEngine(config_path=path)
    _parity(engine, spark)
    result = engine.compute_fee(amount, inst, merch, day)
    import pandas as pd

    df = spark.createDataFrame(
        pd.DataFrame([(amount, inst, merch, day)], columns=["a", "i", "m", "d"]),
        "a bigint, i string, m string, d string",
    )
    fee_sql, gst_sql = build_fee_case_sql(
        engine, amount_col="a", inst_col="i", merchant_col="m", settlement_date_col="d"
    )
    r = df.selectExpr(f"{fee_sql} AS f", f"{gst_sql} AS g").collect()[0]
    want = (amount * result.rate_bps + 5000) // 10000
    assert r.f == want, f"SQL {r.f} vs engine {want}"
    assert amount - r.f - r.g == result.net_paise


def test_merchant_key_with_backslash_is_escaped(tmp_path, spark):
    import pandas as pd

    body = {
        "default": {"mdr_rate_bps": 150, "gst_on_mdr": 0.0, "tolerance_paise": 1},
        "instruments": {"UPI": {"mdr_rate_bps": 900, "gst_on_mdr": 0.0}},
        "merchants": {"ACME\\b": {"UPI": {"mdr_rate_bps": 5, "gst_on_mdr": 0.0}}},
    }
    engine = FeeEngine(config_path=_write_config(tmp_path, "bs.yaml", body))
    _parity(engine, spark)
    df = spark.createDataFrame(
        pd.DataFrame([(100000, "UPI", "ACME\\b", "2025-06-15")], columns=["a", "i", "m", "d"]),
        "a bigint, i string, m string, d string",
    )
    fee_sql, _ = build_fee_case_sql(
        engine, amount_col="a", inst_col="i", merchant_col="m", settlement_date_col="d"
    )
    r = df.selectExpr(f"{fee_sql} AS f").collect()[0]
    assert r.f == 50, f"merchant leg did not match: {r.f} (want 5 bps = 50 paise)"


def test_empty_merchant_key_emits_no_leg(tmp_path, spark):
    import pandas as pd

    body = {
        "default": {"mdr_rate_bps": 150, "gst_on_mdr": 0.0, "tolerance_paise": 1},
        "instruments": {"UPI": {"mdr_rate_bps": 150, "gst_on_mdr": 0.0}},
        "merchants": {"": {"UPI": {"mdr_rate_bps": 0, "gst_on_mdr": 0.0}}},
    }
    engine = FeeEngine(config_path=_write_config(tmp_path, "empty.yaml", body))
    _parity(engine, spark)
    df = spark.createDataFrame(
        pd.DataFrame([(100000, "UPI", "", "2025-06-15")], columns=["a", "i", "m", "d"]),
        "a bigint, i string, m string, d string",
    )
    fee_sql, _ = build_fee_case_sql(
        engine, amount_col="a", inst_col="i", merchant_col="m", settlement_date_col="d"
    )
    r = df.selectExpr(f"{fee_sql} AS f").collect()[0]
    assert r.f == 1500, f"empty merchant key matched: {r.f} (want the 150 bps default = 1500 paise)"
