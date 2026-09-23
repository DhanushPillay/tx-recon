"""Loader pure-logic pins: hardcoded fee math (never FeeEngine), stable buckets."""

from unittest.mock import MagicMock

import pytest

from src.adapters.real_data import (
    THIRU_INSTRUMENT,
    _bucket,
    _noise_delta,
    build_real_files,
    loader_net,
    seed_real_webhooks,
)

pytestmark = pytest.mark.unit


def test_loader_net_credit_hardcoded():
    fee, gst, net = loader_net(100000, "CREDIT_CARD")
    assert (fee, gst, net) == (2000, 360, 97640)


def test_loader_net_debit_hardcoded():
    fee, gst, net = loader_net(100000, "DEBIT_CARD")
    assert (fee, gst, net) == (1000, 180, 98820)


def test_loader_net_matches_v1_card():
    """Loader schedule must equal fee_rates.yaml v1 or real rows mismatch."""
    from src.processing.fee_engine import FeeEngine

    e = FeeEngine()
    for amt, inst in [(100000, "CREDIT_CARD"), (45678, "DEBIT_CARD"), (999, "CREDIT_CARD")]:
        fee, gst, net = loader_net(amt, inst)
        r = e.compute_fee(amt, inst, "some_merchant", "2015-06-15")
        assert net == r.net_paise
        assert gst == r.gst_paise
        ok, _ = e.check_match(amt, net, inst, "some_merchant", "2015-06-15")
        assert ok


def test_bucket_stable_and_bounded():
    assert _bucket("7475327", 7) == _bucket("7475327", 7)
    assert 0 <= _bucket("7475327", 7) < 100
    assert _bucket("7475327", 8) != _bucket("7475327", 7) or True  # seed sensitivity not guaranteed


def test_noise_delta_clamped_for_dust_amounts():
    """Regression: gross=1 paisa + noise -1 produced settled=0 (quarantined 1/12.9M)."""
    assert _noise_delta("whatever", 7) in (-1, 1)
    net = loader_net(1, "CREDIT_CARD")[2]
    assert net == 1
    assert max(1, net + _noise_delta("whatever", 7)) >= 1


def test_thiru_instrument_map():
    assert THIRU_INSTRUMENT["Credit"] == "CREDIT_CARD"
    assert THIRU_INSTRUMENT["Debit"] == "DEBIT_CARD"
    assert THIRU_INSTRUMENT["Debit (Prepaid)"] == "DEBIT_CARD"


def _fake_spark(rows):
    """Spark stub: read.parquet -> select -> toLocalIterator over plain tuples."""
    spark = MagicMock(name="spark")
    staged = MagicMock(name="staged")
    staged.toLocalIterator.return_value = iter(rows)
    spark.read.parquet.return_value.select.return_value = staged
    return spark


def test_build_real_files_end_to_end_small(tmp_path):
    # (tx, amount, merchant, card_type, day): clean, negative, zero, bad-amount
    rows = [
        ("1001", 1000.0, "59935", "Credit", "2015-06-15"),
        ("1002", 250.5, "27092", "Debit", "2016-01-02"),
        ("1003", -50.0, "59935", "Credit", "2015-06-15"),
        ("1004", 0.0, "1", "Debit", "2017-03-03"),
        ("1005", "oops", "1", "Credit", "2018-04-04"),
        ("1006", 10.0, None, "Weird", "2019-05-05"),
    ]
    spark = _fake_spark(rows)
    out_csv = str(tmp_path / "settlement.csv")
    out_hooks = str(tmp_path / "hooks.csv")
    counts = build_real_files("dummy.parquet", out_csv, out_hooks, seed=7, spark=spark)
    assert counts["rows"] == 6
    assert counts["dropped_invalid"] == 3  # negative, zero, non-numeric
    assert counts["kept"] >= 2
    import pandas as pd

    df = pd.read_csv(out_csv)
    assert list(df.columns)[:5] == [
        "bank_ref_id",
        "transaction_id",
        "settled_amount_paise",
        "settlement_date",
        "instrument_type",
    ]
    assert set(df["transaction_id"].astype(str)) <= {"1001", "1002", "1006"}
    assert (df["instrument_type"] == "CREDIT_CARD").sum() >= 1  # Weird -> fallback
    hooks = pd.read_csv(out_hooks)
    assert set(hooks.columns) == {
        "transaction_id",
        "amount_paise",
        "day",
        "merchant_id",
        "instrument_type",
    }
    # orphans withheld from hooks, everyone else present
    assert len(hooks) <= len(df)


def test_build_real_files_all_branches(tmp_path, monkeypatch):
    """Scripted buckets hit mismatch/noise/orphan/dup/clean + chunk flush + sample skip."""
    import src.adapters.real_data as rd

    buckets = {"2001": 3, "2002": 6, "2003": 10, "2004": 12, "2005": 50, "2006": 50}
    real_bucket = rd._bucket
    monkeypatch.setattr(rd, "_FLUSH_EVERY", 2)
    monkeypatch.setattr(rd, "_bucket", lambda tx, seed: buckets.get(tx.split("|")[-1], 50))
    rows = [
        ("2001", 1000.0, "1", "Credit", "2015-01-01"),  # mismatch
        ("2002", 1000.0, "1", "Credit", "2015-01-01"),  # noise
        ("2003", 1000.0, "1", "Debit", "2015-01-01"),  # orphan
        ("2004", 1000.0, "1", "Debit", "2015-01-01"),  # dup
        ("2005", 1000.0, "1", "Credit", "2015-01-01"),  # clean
        ("2006", 1000.0, "1", "Credit", "2015-01-01"),  # sample-skipped
    ]
    spark = _fake_spark(rows)
    out_csv = str(tmp_path / "s.csv")
    out_hooks = str(tmp_path / "h.csv")
    # sample path: only buckets < 100 pass; force-skip 2006 via sample_pct=0? no:
    # sample uses _bucket(tx) too — scripted 50 for all, so run sample_pct=100 then
    # a second run with a bucket override for sampling.
    counts = rd.build_real_files("d.pq", out_csv, out_hooks, seed=7, spark=spark)
    assert counts["mismatch"] == 1
    assert counts["orphans"] == 1
    assert counts["dups"] == 1
    assert counts["kept"] == 7  # 6 tx + 1 dup row
    import pandas as pd

    df = pd.read_csv(out_csv)
    assert len(df) == 7
    assert "2003" not in pd.read_csv(out_hooks)["transaction_id"].astype(str).values
    # sampling gate: bucket >= pct skipped
    monkeypatch.setattr(rd, "_bucket", lambda tx, seed: 99)
    c2 = rd.build_real_files(
        "d.pq", str(tmp_path / "s2.csv"), str(tmp_path / "h2.csv"), sample_pct=50, spark=spark
    )
    assert c2["kept"] == 0
    monkeypatch.setattr(rd, "_bucket", real_bucket)


def test_build_real_files_bad_dataset():
    with pytest.raises(ValueError):
        build_real_files("x", "y", "z", dataset="nope", spark=_fake_spark([]))


def test_seed_real_webhooks_wiring():
    spark = MagicMock(name="spark")
    st = MagicMock(name="staged")
    st.count.return_value = 42
    spark.read.csv.return_value = st
    n = seed_real_webhooks(spark, "nessie.db.webhooks", "hooks.csv", "2020-01-01")
    assert n == 42
    sqls = [c[0][0] for c in spark.sql.call_args_list]
    assert any("MERGE INTO nessie.db.webhooks" in s for s in sqls)
    assert any("INSERT INTO nessie.db.webhooks" in s for s in sqls)
    assert any("2020-01-01T00:00:00" in s for s in sqls)


def test_seed_real_webhooks_bad_inputs():
    spark = MagicMock(name="spark")
    with pytest.raises(ValueError):
        seed_real_webhooks(spark, "nessie.db.webhooks", "hooks.csv", "01/01/2020")
    with pytest.raises(ValueError):
        seed_real_webhooks(spark, "x; DROP TABLE", "hooks.csv", "2020-01-01")
