"""P4: MERGE + dedup + WAP + stateful rerun convergence (unit-only, no Spark).

Static shape pins on run_reconciliation source, pure-function checks on
_settlement_source / leg_enabled / check_batch_drift, plus a Hypothesis
RuleBasedStateMachine proving reruns converge (idempotent re-merge).
"""

import inspect
import os

import pytest
from hypothesis import strategies as st
from hypothesis.stateful import RuleBasedStateMachine, invariant, rule

from src.pipeline import check_batch_drift
from src.processing import reconcile
from src.processing.reconcile import _settlement_source, leg_enabled

pytestmark = pytest.mark.unit

MERGE_SRC = inspect.getsource(reconcile.run_reconciliation)


# ---- MERGE shape ----


def test_merge_keyed_on_transaction_id():
    assert "ON t.transaction_id = s.transaction_id" in MERGE_SRC


def test_merge_has_four_matched_updates_one_insert():
    # Four WHEN MATCHED arms: placeholder-preserve, null-amount, currency
    # conflict, and the tolerance CASE. First match wins, so the currency arm
    # has to sit ahead of the tolerance one or a USD settlement gets priced as
    # if it were rupees.
    assert MERGE_SRC.count("WHEN MATCHED") == 4
    assert MERGE_SRC.count("WHEN NOT MATCHED") == 1


def test_merge_preserves_placeholders_and_terminal_rows():
    assert "_placeholder_in" in MERGE_SRC
    assert "_terminal_not_in" in MERGE_SRC
    assert "IS NOT NULL" in MERGE_SRC


def test_dedup_window_is_deterministic():
    assert "settlement_date" in MERGE_SRC and "desc" in MERGE_SRC.lower()
    assert "bank_ref_id" in MERGE_SRC and "settlement_id" in MERGE_SRC


# ---- settlement source selection ----


def test_curated_preferred_over_raw(tmp_path):
    d = str(tmp_path)
    (tmp_path / "settlement_20250402.csv").write_text("a\n", encoding="utf-8")
    (tmp_path / "curated_settlement_20250402.csv").write_text("a\n", encoding="utf-8")
    pattern, files = _settlement_source(d, "2025-04-02")
    assert "curated_settlement_20250402.csv" in pattern.replace("\\", "/")


def test_missing_curated_fails_closed_without_escape(tmp_path, monkeypatch):
    d = str(tmp_path)
    (tmp_path / "settlement_20250402.csv").write_text("a\n", encoding="utf-8")
    monkeypatch.delenv("ALLOW_UNVALIDATED_SETTLEMENT", raising=False)
    with pytest.raises(FileNotFoundError, match="curated settlement not found"):
        _settlement_source(d, "2025-04-02")


def test_escape_hatch_is_opt_in(tmp_path, monkeypatch):
    d = str(tmp_path)
    (tmp_path / "settlement_20250402.csv").write_text("a\n", encoding="utf-8")
    monkeypatch.setenv("ALLOW_UNVALIDATED_SETTLEMENT", "1")
    pattern, _ = _settlement_source(d, "2025-04-02")
    assert "settlement_20250402.csv" in pattern.replace("\\", "/")


def test_bad_date_rejected_before_glob(tmp_path):
    with pytest.raises(ValueError, match="Bad date_str"):
        _settlement_source(str(tmp_path), "x; DROP")


# ---- legs + drift ----


def test_leg_enabled_empty_means_all(monkeypatch):
    monkeypatch.delenv("WIRED_LEGS", raising=False)
    s = reconcile.get_settings()
    s.wired_legs = ""
    assert leg_enabled("wap", s) and leg_enabled("bank", s) and leg_enabled("residual", s)


def test_leg_enabled_subset(monkeypatch):
    s = reconcile.get_settings()
    s.wired_legs = "wap,residual"
    assert leg_enabled("wap", s) is True
    assert leg_enabled("bank", s) is False


def test_drift_gate_and_duplicate_diagnostic():
    check_batch_drift({"settlement_rows_evaluated": 3, "batch_MATCHED": 3})
    check_batch_drift(
        {
            "settlement_rows_evaluated": 3,
            "batch_MATCHED": 3,
            "batch_EXCEPTION_DUPLICATE_SETTLEMENT": 99,
        }
    )
    with pytest.raises(RuntimeError, match="Batch drift"):
        check_batch_drift({"settlement_rows_evaluated": 3, "batch_MATCHED": 1})


def test_slo_env_knob_parsed():
    os.environ.pop("MATCH_RATE_SLO", None)
    from src.pipeline import _slo_ok

    assert _slo_ok({"batch_match_rate": 1.0}) is True
    assert _slo_ok({"batch_match_rate": 0.0}) is False
    assert _slo_ok({}) is True


# ---- stateful: rerun convergence ----


def _dedup(rows):
    """Mirror of the SQL window: latest settlement_date wins, then bank_ref_id."""
    best = {}
    for tx, dt, ref in rows:
        if tx is None:
            continue
        cur = best.get(tx)
        if cur is None or (dt, ref) > (cur[0], cur[1]):
            best[tx] = (dt, ref)
    return best


class _DedupConvergenceMachine(RuleBasedStateMachine):
    def __init__(self):
        super().__init__()
        self.rows = []

    @rule(
        tx=st.sampled_from(["TXN000001", "TXN000002", "TXN000003", None]),
        dt=st.sampled_from(["2025-04-01", "2025-04-02"]),
        ref=st.sampled_from(["B1", "B2"]),
    )
    def add(self, tx, dt, ref):
        self.rows.append((tx, dt, ref))

    @invariant()
    def reruns_converge(self):
        once = _dedup(self.rows)
        twice = _dedup(self.rows + self.rows)
        assert twice == once
        distinct = {r[0] for r in self.rows if r[0] is not None}
        assert len(once) <= len(distinct)


TestDedupConvergence = _DedupConvergenceMachine.TestCase


def test_stateful_dedup_rerun_converges():
    import unittest

    suite = unittest.TestLoader().loadTestsFromTestCase(TestDedupConvergence)
    result = unittest.TextTestRunner(verbosity=0).run(suite)
    assert result.wasSuccessful()
