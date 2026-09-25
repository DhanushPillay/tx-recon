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


def test_gap_date_uses_prior_card(tmp_path):
    """Gap dates price with the last-prior card (mirrors SQL _versioned_wrap)."""
    cfg = {
        "version": "v2.0.0",
        "default": {"mdr_rate_bps": 150, "gst_on_mdr": 18.0, "tolerance_paise": 1},
        "rate_card_history": [
            {
                "version": "v1.0.0",
                "effective_from": "2024-01-01",
                "effective_to": "2024-12-31",
                "default": {"mdr_rate_bps": 100, "gst_on_mdr": 18.0, "tolerance_paise": 1},
            },
            {
                "version": "v2.0.0",
                "effective_from": "2025-06-01",
                "default": {"mdr_rate_bps": 300, "gst_on_mdr": 18.0, "tolerance_paise": 1},
            },
        ],
    }
    p = _write_yaml(cfg, tmp_path / "rates.yaml")
    engine = FeeEngine(config_path=str(p))
    # 2025-03-15 is in neither range -> last-prior card (v1, 100 bps), not latest.
    r = engine.get_rate_for_date("UPI", None, "2025-03-15")
    assert r["mdr_rate_bps"] == 100
    # Pre-first -> earliest card.
    r0 = engine.get_rate_for_date("UPI", None, "2020-01-01")
    assert r0["mdr_rate_bps"] == 100


def test_overlapping_cards_rejected(tmp_path):
    import pytest

    cfg = {
        "version": "v2.0.0",
        "default": {"mdr_rate_bps": 150, "gst_on_mdr": 18.0, "tolerance_paise": 1},
        "rate_card_history": [
            {
                "version": "v1.0.0",
                "effective_from": "2024-01-01",
                "effective_to": "2025-06-30",
                "default": {"mdr_rate_bps": 100, "gst_on_mdr": 18.0, "tolerance_paise": 1},
            },
            {
                "version": "v2.0.0",
                "effective_from": "2025-04-01",
                "default": {"mdr_rate_bps": 300, "gst_on_mdr": 18.0, "tolerance_paise": 1},
            },
        ],
    }
    p = _write_yaml(cfg, tmp_path / "rates.yaml")
    with pytest.raises(ValueError, match="overlap"):
        FeeEngine(config_path=str(p))


def test_merchant_merge_card_wins_over_top(tmp_path):
    """Merchant in both levels: card rate wins (same merge as SQL builder)."""
    cfg = {
        "version": "v1.0.0",
        "default": {"mdr_rate_bps": 150, "gst_on_mdr": 18.0, "tolerance_paise": 1},
        "merchants": {"merch_001": {"CREDIT_CARD": {"mdr_rate_bps": 180}}},
        "rate_card_history": [
            {
                "version": "v1.0.0",
                "effective_from": "2024-01-01",
                "default": {"mdr_rate_bps": 150, "gst_on_mdr": 18.0, "tolerance_paise": 1},
                "merchants": {"merch_001": {"CREDIT_CARD": {"mdr_rate_bps": 190}}},
            }
        ],
    }
    p = _write_yaml(cfg, tmp_path / "rates.yaml")
    engine = FeeEngine(config_path=str(p))
    assert engine.get_rate_for_date("CREDIT_CARD", "merch_001", "2024-06-01")["mdr_rate_bps"] == 190
    # Merchant only at top level still resolves.
    assert engine.get_rate_for_date("UPI", "merch_001", "2024-06-01")["mdr_rate_bps"] == 150


def test_versioned_sql_routes_gap_to_prior():
    """SQL CASE has no upper bounds: gap dates fall to the prior card, NULL to latest."""
    from src.processing.reconcile import _versioned_wrap

    cards = [
        {
            "version": "v1",
            "effective_from": "2024-01-01",
            "effective_to": "2024-12-31",
            "default": {"mdr_rate_bps": 100, "gst_on_mdr": 18.0, "tolerance_paise": 1},
        },
        {
            "version": "v2",
            "effective_from": "2025-06-01",
            "default": {"mdr_rate_bps": 300, "gst_on_mdr": 18.0, "tolerance_paise": 1},
        },
    ]
    (case,) = _versioned_wrap(
        cards, {}, "s.settlement_date", lambda c: (str(c["default"]["mdr_rate_bps"]),)
    )
    assert "2024-12-31" not in case, "upper bound must not route gap dates to latest"
    assert "IS NULL" in case
    # Latest-first ordering: v2 WHEN precedes v1 WHEN.
    assert case.index("2025-06-01") < case.index("2024-01-01")
