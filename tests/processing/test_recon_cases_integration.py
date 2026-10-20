"""The same fixture, run through the real MERGE against the real catalog.

``tests/test_recon_cases.py`` evaluates the batch MERGE's decision as a SELECT,
which covers every arm and every guard but never executes the MERGE itself.
Spark's session catalog refuses ``MERGE INTO TABLE`` and the Iceberg catalog
needs the docker services, so the statement is only reachable here.

Same YAML, same hand-written labels. If the two tiers disagree, the SELECT and
the MERGE have drifted and one of them is lying.

Marked ``integration`` because it needs Redpanda, MinIO and Nessie up.
"""

from __future__ import annotations

import csv
import os

import pytest
import yaml

from src.common.config import get_spark_session
from src.common.schemas import EXCEPTION_REVERSAL, PENDING_RECONCILIATION
from src.common.settings import get_settings
from src.ingestion import ingest_webhooks as iw
from src.processing.reconcile import run_reconciliation

pytestmark = pytest.mark.integration

HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FIXTURE = os.path.join(HERE, "fixtures", "recon_cases.yaml")

# The fixture's batch date, and the file stem _settlement_source looks for.
STAMP = "20250610"


def _fx() -> dict:
    with open(FIXTURE, encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def _lit(value) -> str:
    return "NULL" if value is None else f"'{value}'"


def _scope() -> str:
    """A predicate that selects this fixture's rows and nothing else."""
    ids = ", ".join(f"'{c['id']}'" for c in _fx()["cases"])
    return f"transaction_id IN ({ids})"


def _ingestion_status(w: dict) -> str:
    """What ingestion writes before reconciliation ever sees the row.

    A negative amount with a reversal word parks at EXCEPTION_REVERSAL;
    everything else arrives pending. ``iw._ingest_status_column`` is the
    column; this is its literal, so the INSERT below is readable SQL.
    """
    reversal = w["amount_paise"] is not None and w["amount_paise"] <= 0
    return EXCEPTION_REVERSAL if reversal else PENDING_RECONCILIATION


def _write_curated(data_dir: str) -> str:
    """The settlement rows as the post-validation file the batch merges from.

    ``_settlement_source`` fails closed without a curated file, which is the
    point of it, so the fixture writes the canonical shape directly rather than
    going through validation. Nothing here needs validating: every value is one
    this file chose.
    """
    from src.adapters.settlement import CANONICAL_COLS

    fields = list(CANONICAL_COLS) + ["provider"]
    path = os.path.join(data_dir, f"curated_settlement_{STAMP}.csv")
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        for row in _fx()["settlements"]:
            rec = dict(row)
            rec["transaction_id"] = row["tx"]
            rec["provider"] = "gateway"
            writer.writerow({c: ("null" if rec.get(c) is None else rec.get(c)) for c in fields})
    return path


@pytest.fixture(scope="module")
def batch():
    """Run the batch once, and hand back the statuses and the counters.

    One run for the whole module: a batch against a live catalog takes minutes,
    and every assertion here is about that same run.
    """
    # The fixture's timestamps are all in the past, so the late-SLA sweep would
    # age its placeholders to EXCEPTION_LATE_UNRESOLVED before anything is
    # asserted. Ageing is its own concern with its own tests; here it would only
    # obscure the arms under test.
    previous = os.environ.get("LATE_SLA_DAYS")
    os.environ["LATE_SLA_DAYS"] = "100000"

    settings = get_settings()
    spark = get_spark_session("ReconCasesIntegration")
    iw.ensure_webhook_table(spark, settings.webhook_table)

    scope = _scope()
    spark.sql(f"DELETE FROM {settings.webhook_table} WHERE {scope}")
    values = ",".join(
        f"('{w['id']}', {_lit(w['amount_paise'])}, '{w['gateway_status']}', "
        f"'{w['timestamp_utc']}', {_lit(w['merchant_id'])}, 'recon-cases', "
        f"'{_ingestion_status(w)}', NULL, {_lit(w['instrument_type'])}, "
        f"{_lit(w['currency'])}, current_timestamp(), NULL, NULL)"
        for w in _fx()["webhooks"]
    )
    spark.sql(
        f"INSERT INTO {settings.webhook_table} ("
        "transaction_id, amount_paise, gateway_status, timestamp_utc, merchant_id, "
        "processing_run_id, reconciliation_status, bank_ref_id, instrument_type, "
        f"currency, ingested_at, duplicate_mutation_count, last_duplicate_at) VALUES {values}"
    )

    data_dir = os.path.join(settings.project_root, "data")
    os.makedirs(data_dir, exist_ok=True)
    seed = _write_curated(data_dir)
    try:
        counts = run_reconciliation(STAMP)
        rows = spark.sql(
            f"SELECT transaction_id, reconciliation_status FROM {settings.webhook_table} "
            f"WHERE {scope}"
        ).collect()
    finally:
        if os.path.exists(seed):
            os.remove(seed)
        spark.sql(f"DELETE FROM {settings.webhook_table} WHERE {scope}")
        if previous is None:
            os.environ.pop("LATE_SLA_DAYS", None)
        else:
            os.environ["LATE_SLA_DAYS"] = previous

    return {
        "statuses": {r["transaction_id"]: r["reconciliation_status"] for r in rows},
        "counts": counts,
    }


def test_the_batch_produces_a_row_for_every_labelled_transaction(batch):
    reconciled = batch["statuses"]
    expected = {c["id"] for c in _fx()["cases"]}
    assert set(reconciled) == expected, (
        f"missing: {sorted(expected - set(reconciled))}; "
        f"unexpected: {sorted(set(reconciled) - expected)}"
    )


def test_every_transaction_lands_on_the_status_the_fixture_writes_down(batch):
    """One failure block, naming the case and the reason it was labelled.

    Forty-one separate assertion failures would bury which rule broke. One
    failure that says "h_diff: currency arm, both sides declared INR and USD" is
    a diagnosis.
    """
    reconciled = batch["statuses"]
    lines = []
    for case in _fx()["cases"]:
        got = reconciled.get(case["id"])
        if got != case["expect_status"]:
            lines.append(
                f"{case['id']}: fixture says {case['expect_status']}, batch produced {got}"
            )
            lines.append(f"    why: {case['why']}")
    assert not lines, "\n".join(lines)


def test_the_batch_reports_the_counts_the_fixture_writes_down(batch):
    """The counters the fixture states, from a real run rather than a SELECT."""
    wanted = _fx()["expected_counts"]
    wrong = {
        key: (want, batch["counts"].get(key))
        for key, want in wanted.items()
        if batch["counts"].get(key) != want
    }
    assert not wrong, "\n".join(
        f"{key}: fixture says {want}, the batch reported {got}"
        for key, (want, got) in wrong.items()
    )
