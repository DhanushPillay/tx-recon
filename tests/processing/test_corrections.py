"""Corrections journal: append-only audit for manual money mutations."""

import json

import pytest

from src.processing.corrections import journal_correction

pytestmark = pytest.mark.unit


def test_journal_appends(tmp_path):
    p = journal_correction(str(tmp_path), "dlq_purge", "TICKET-1", "alice", {"rows": 3})
    assert p.endswith("corrections.jsonl")
    entry = json.loads((tmp_path / "corrections.jsonl").read_text().strip().splitlines()[-1])
    assert entry["action"] == "dlq_purge"
    assert entry["reason"] == "TICKET-1"
    assert entry["approved_by"] == "alice"
    assert entry["details"] == {"rows": 3}
    journal_correction(str(tmp_path), "rematch", "TICKET-2", "bob")
    assert len((tmp_path / "corrections.jsonl").read_text().strip().splitlines()) == 2


def test_journal_refuses_without_reason_or_approver(tmp_path):
    with pytest.raises(ValueError, match="reason"):
        journal_correction(str(tmp_path), "dlq_purge", "", "alice")
    with pytest.raises(ValueError, match="approved_by"):
        journal_correction(str(tmp_path), "dlq_purge", "TICKET-1", "  ")
    assert not (tmp_path / "corrections.jsonl").exists()


def test_journal_write_failure_raises(tmp_path):
    """Unwritable journal dir fails the mutation, never silently unlogged."""
    blocker = tmp_path / "blocker"
    blocker.write_text("not-a-dir")
    with pytest.raises(RuntimeError, match="cannot write corrections journal"):
        journal_correction(str(blocker), "dlq_purge", "TICKET-1", "alice")
