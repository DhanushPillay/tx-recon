"""Provider batch model: config loading, batch identity, lag windows."""

import pytest

from src.processing.batches import assign_batch, load_providers, provider_spec, within_lag

pytestmark = pytest.mark.unit


def test_load_providers_real_config():
    cfg = load_providers()
    assert "razorpay" in cfg["providers"]
    assert provider_spec(cfg, "razorpay")["lag_days"] == 2
    assert provider_spec(cfg, "nope")["late_sla_days"] == 7  # unknown -> defaults


def test_load_providers_missing_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        load_providers(str(tmp_path / "nope.yaml"))


def test_load_providers_malformed_raises(tmp_path):
    p = tmp_path / "bad.yaml"
    p.write_text("just: [a, list]\n")
    with pytest.raises(ValueError, match="malformed"):
        load_providers(str(p))


def test_assign_batch():
    assert assign_batch("razorpay", "2025-04-02") == "razorpay:2025-04-02"
    assert assign_batch(None, "2025-04-02") == "gateway:2025-04-02"


def test_within_lag():
    assert within_lag("2025-04-01", "2025-04-02", 2) is True  # next-day batch
    assert within_lag("2025-04-02", "2025-04-02", 2) is True
    assert within_lag("2025-03-28", "2025-04-02", 2) is False  # beyond lag
    assert within_lag("2025-04-03", "2025-04-02", 2) is False  # webhook after settlement
    assert within_lag("bad-date", "2025-04-02", 2) is False


def test_provider_spec_ignores_non_dict_override(tmp_path):
    cfg = {"default": {"lag_days": 2}, "providers": {"x": "not-a-dict"}}
    assert provider_spec(cfg, "x")["lag_days"] == 2
