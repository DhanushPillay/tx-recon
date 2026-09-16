"""Fee engine boundaries + versioning edges."""

import pytest

from src.processing.fee_engine import FeeEngine, _parse_iso_date

pytestmark = pytest.mark.unit


def test_parse_iso_invalid_returns_none():
    assert _parse_iso_date("not-a-date") is None
    assert _parse_iso_date("2025-13-99") is None


def test_zero_and_full_bps():
    e = FeeEngine()
    r0 = e.compute_fee(100000, "UPI")  # UPI 0bps in default config
    assert r0.net_paise <= 100000
    with pytest.raises(ValueError):
        e.compute_fee(-1, "UPI")


def test_gst_zero_branch(tmp_path):
    p = tmp_path / "rates.yaml"
    p.write_text(
        "version: v1\ndefault:\n  mdr_rate_bps: 100\n  gst_on_mdr: 0\n  tolerance_paise: 1\ninstruments: {}\n"
    )
    e = FeeEngine(config_path=str(p))
    r = e.compute_fee(100000, "UPI")
    assert r.gst_paise == 0


def test_invalid_config_raises(tmp_path):
    p = tmp_path / "bad.yaml"
    p.write_text(
        "version: v1\ndefault:\n  mdr_rate_bps: 99999\n  gst_on_mdr: 18\n  tolerance_paise: 1\n"
    )
    with pytest.raises(ValueError):
        FeeEngine(config_path=str(p))


def test_history_aliases_and_pre_earliest(tmp_path):
    p = tmp_path / "hist.yaml"
    p.write_text(
        "rate_card_history:\n"
        "  - version: v1\n    effective_from: 2024-01-01\n    effective_to: 2024-12-31\n"
        "    default: {mdr_rate_bps: 100, gst_on_mdr: 18, tolerance_paise: 1}\n"
        "  - version: v2\n    effective_from: 2025-01-01\n"
        "    default: {mdr_rate_bps: 200, gst_on_mdr: 18, tolerance_paise: 1}\n"
    )
    e = FeeEngine(config_path=str(p))
    assert e.get_rate_for_date("UPI", settlement_date="2020-01-01")["mdr_rate_bps"] == 100
    assert e.get_rate_for_date("UPI", settlement_date="bad-date")["mdr_rate_bps"] == 200
    assert e.get_rate_for_date("UPI", settlement_date="2025-06-01")["mdr_rate_bps"] == 200


def test_merchant_fallback_top_level():
    e = FeeEngine()
    e.config.setdefault("merchants", {})["m_test"] = {"UPI": {"mdr_rate_bps": 50}}
    assert e.get_rate("UPI", "m_test")["mdr_rate_bps"] == 50


def test_effective_to_boundary():
    e = FeeEngine()  # real config/fee_rates.yaml has v1..2025-03-31, v2 from 2025-04-01
    v1 = e.get_rate_for_date("CREDIT_CARD", settlement_date="2025-03-31")
    v2 = e.get_rate_for_date("CREDIT_CARD", settlement_date="2025-04-01")
    assert v1 and v2


@pytest.mark.parametrize(
    "default_patch",
    [
        {},
        {"gst_on_mdr": 18, "tolerance_paise": 1},
        {"mdr_rate_bps": 100, "gst_on_mdr": -1, "tolerance_paise": 1},
        {"mdr_rate_bps": 100, "gst_on_mdr": 18, "tolerance_paise": -1},
    ],
)
def test_validate_default_arms(tmp_path, default_patch):
    import yaml

    cfg = {"version": "v1", "default": default_patch, "instruments": {}}
    p = tmp_path / "c.yaml"
    p.write_text(yaml.safe_dump(cfg))
    with pytest.raises(ValueError):
        FeeEngine(config_path=str(p))


@pytest.mark.parametrize(
    "instruments,merchants",
    [
        ({"UPI": {"mdr_rate_bps": 99999}}, {}),
        ({}, {"m1": "not-a-dict"}),
        ({}, {"m1": {"UPI": {"mdr_rate_bps": -5}}}),
    ],
)
def test_validate_nested_arms(tmp_path, instruments, merchants):
    import yaml

    cfg = {
        "version": "v1",
        "default": {"mdr_rate_bps": 100, "gst_on_mdr": 18, "tolerance_paise": 1},
        "instruments": instruments,
        "merchants": merchants,
    }
    p = tmp_path / "c.yaml"
    p.write_text(yaml.safe_dump(cfg))
    with pytest.raises(ValueError):
        FeeEngine(config_path=str(p))


@pytest.mark.parametrize(
    "history",
    [
        "not-a-list",
        [],
        [{"version": "v1"}],
        [
            {
                "version": "v1",
                "effective_from": "bad-date",
                "default": {"mdr_rate_bps": 100, "gst_on_mdr": 18, "tolerance_paise": 1},
            }
        ],
        [
            {
                "version": "v1",
                "effective_from": "2024-01-01",
                "effective_to": "bad-date",
                "default": {"mdr_rate_bps": 100, "gst_on_mdr": 18, "tolerance_paise": 1},
            }
        ],
    ],
)
def test_validate_history_arms(tmp_path, history):
    import yaml

    cfg = {"version": "v1", "history": history}
    p = tmp_path / "c.yaml"
    p.write_text(yaml.safe_dump(cfg))
    with pytest.raises(ValueError):
        FeeEngine(config_path=str(p))
