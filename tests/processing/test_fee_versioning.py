import yaml

from src.processing.fee_engine import FeeEngine


def _write_yaml(data, path):
    with open(path, "w") as f:
        yaml.safe_dump(data, f)
    return path


def test_versioned_lookup_picks_effective_card(tmp_path):
    cfg = {
        "version": "v2.0.0",
        "default": {"mdr_rate_bps": 150, "gst_on_mdr": 18.0, "tolerance_paise": 1},
        "instruments": {"CREDIT_CARD": {"mdr_rate_bps": 200, "gst_on_mdr": 18.0}},
        "rate_card_history": [
            {
                "version": "v1.0.0",
                "effective_from": "2024-01-01",
                "effective_to": "2025-03-31",
                "default": {"mdr_rate_bps": 150, "gst_on_mdr": 18.0, "tolerance_paise": 1},
                "instruments": {"CREDIT_CARD": {"mdr_rate_bps": 200}},
            },
            {
                "version": "v2.0.0",
                "effective_from": "2025-04-01",
                "default": {"mdr_rate_bps": 150, "gst_on_mdr": 18.0, "tolerance_paise": 1},
                "instruments": {"CREDIT_CARD": {"mdr_rate_bps": 250}},
            },
        ],
    }
    p = _write_yaml(cfg, tmp_path / "rates.yaml")
    engine = FeeEngine(config_path=str(p))
    # Before cutover -> 200 bps
    r1 = engine.get_rate_for_date("CREDIT_CARD", None, "2025-03-15")
    assert r1["mdr_rate_bps"] == 200
    # After cutover -> 250 bps
    r2 = engine.get_rate_for_date("CREDIT_CARD", None, "2025-04-15")
    assert r2["mdr_rate_bps"] == 250
    # compute_fee with date picks versioned rate
    fee_old = engine.compute_fee(100000, "CREDIT_CARD", settlement_date="2025-03-15")
    fee_new = engine.compute_fee(100000, "CREDIT_CARD", settlement_date="2025-04-15")
    assert fee_old.fee_paise != fee_new.fee_paise
    assert fee_old.rate_version == "v1.0.0"
    assert fee_new.rate_version == "v2.0.0"


def test_merchant_fallback_to_root_config(tmp_path):
    cfg = {
        "version": "v1.0.0",
        "default": {"mdr_rate_bps": 150, "gst_on_mdr": 18.0, "tolerance_paise": 1},
        "instruments": {"CREDIT_CARD": {"mdr_rate_bps": 200}},
        "merchants": {"merch_001": {"CREDIT_CARD": {"mdr_rate_bps": 180}}},
        "rate_card_history": [
            {
                "version": "v1.0.0",
                "effective_from": "2024-01-01",
                "default": {"mdr_rate_bps": 150, "gst_on_mdr": 18.0, "tolerance_paise": 1},
                "instruments": {"CREDIT_CARD": {"mdr_rate_bps": 200}},
            }
        ],
    }
    p = _write_yaml(cfg, tmp_path / "rates.yaml")
    engine = FeeEngine(config_path=str(p))
    # History card lacks merchants -> should fallback to top-level merchants
    r = engine.get_rate_for_date("CREDIT_CARD", "merch_001", "2024-06-01")
    assert r["mdr_rate_bps"] == 180


def test_backward_compat_single_card_file(tmp_path):
    cfg = {
        "version": "v1.0.0",
        "default": {"mdr_rate_bps": 150, "gst_on_mdr": 18.0, "tolerance_paise": 1},
        "instruments": {"UPI": {"mdr_rate_bps": 0, "gst_on_mdr": 0}},
    }
    p = _write_yaml(cfg, tmp_path / "rates.yaml")
    engine = FeeEngine(config_path=str(p))
    r = engine.get_rate_for_date("UPI", None, "2026-01-01")
    assert r["mdr_rate_bps"] == 0
    assert engine.config_version == "v1.0.0"
