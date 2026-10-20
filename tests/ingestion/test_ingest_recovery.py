"""Three ways the ingestion path used to hide trouble instead of reporting it.

* A schema drift and a PAN in the payload are both refused deliberately. The
  supervisor retried them like a broker blip, so five replays and about nine
  minutes of backoff ended at the same refusal.
* The schema-id check caught its own failure and carried on. An empty id list
  made the guard fall through, so a batch nobody could verify went into the
  MERGE with any new producer field quietly dropped.
* The DLQ keyed on ``transaction_id``. A record without one matched nothing on
  replay, because ``NULL != NULL``, so every replay appended another row and
  ``replay_dlq.py --delete`` could never reach any of them.

These are all in ``ingest_webhooks.py``; nothing here needs a broker.
"""

from __future__ import annotations

import inspect

from src.ingestion import ingest_webhooks as iw

# --- the retry decision ----------------------------------------------------


def test_a_refusal_a_retry_cannot_fix_has_its_own_type():
    assert issubclass(iw.PermanentIngestError, RuntimeError), (
        "existing callers that catch RuntimeError around run_ingestion must keep working"
    )


def test_the_supervisor_stops_on_it_instead_of_retrying():
    src = inspect.getsource(iw.run_ingestion)
    permanent = src.index("except PermanentIngestError:")
    generic = src.index("except Exception:")
    assert permanent < generic, "the specific handler has to come first or the bare one swallows it"
    between = src[permanent:generic]
    assert "_attempt" not in between, (
        "the permanent branch must not consume an attempt or sleep: the whole "
        "point is that the five replays do not happen"
    )


def test_both_deliberate_refusals_are_permanent():
    """The two raise sites, matched on their message so a rewrite is visible."""
    src = inspect.getsource(iw.run_ingestion)
    for marker in ("schema-id drift: batch ids", "PAN detected in streaming batch"):
        assert marker in src, f"{marker} is gone; this test needs updating"
        # The message is inside the raise, so look at what precedes it.
        head = src[: src.index(marker)]
        stmt = head[head.rindex("raise ") :]
        assert stmt.startswith("raise PermanentIngestError("), (
            f"{marker} is raised as something the supervisor will retry, and a retry cannot fix it"
        )
        assert "raise RuntimeError(" not in stmt, marker


# --- the schema-id guard ---------------------------------------------------


def test_the_schema_id_check_no_longer_fails_open():
    src = inspect.getsource(iw.run_ingestion)
    block = src[src.index("_ids = sorted(") : src.index("if _ids and ")]
    assert "except" in block, "the guard has no failure path at all; check this test still fits"
    handler = block[block.index("except") :]
    bare = [ln.strip() for ln in handler.splitlines() if ln.strip() == "raise"]
    assert bare, (
        "the handler must re-raise. Swallowing it leaves _ids empty, the guard "
        "falls through, and an unverified batch goes into the MERGE with any "
        "new producer field dropped."
    )
    assert "_ids = []" not in handler, "an empty id list is what makes the guard fall through"
    assert "schema-id check failed" in handler, "the failure has to be logged, not silent"


def test_an_unverifiable_batch_is_still_retried():
    """Failing to read the id is a Spark problem, not a config one.

    It raises rather than continuing, but it must not be typed permanent: a
    transient collect failure recovers on the next attempt.
    """
    src = inspect.getsource(iw.run_ingestion)
    assert "schema-id check failed, batch not verified" in src
    block = src[src.index("schema-id check failed") :]
    assert "PermanentIngestError" not in block.split("if _ids")[0]


# --- the DLQ key -----------------------------------------------------------


def test_the_dlq_key_is_content_not_a_nullable_id(spark):
    """A hash of the row's own fields, so it is present and stable."""
    from pyspark.sql.types import LongType, StringType, StructField, StructType

    schema = StructType(
        [StructField(c, LongType() if "paise" in c else StringType()) for c in iw._DLQ_COLS]
    )
    rows = [
        (None, 100, "SUCCESS", "2026-06-10T00:00:00", "m1", "r1"),
        (None, 100, "SUCCESS", "2026-06-10T00:00:00", "m1", "r1"),
        (None, 200, "SUCCESS", "2026-06-10T00:00:00", "m1", "r1"),
    ]
    out = spark.createDataFrame(rows, schema).withColumn(
        "payload_hash", iw._dlq_dedupe_hash(iw._DLQ_COLS)
    )
    hashes = [r["payload_hash"] for r in out.collect()]
    assert len(hashes) == 3
    assert all(h and len(h) == 64 for h in hashes), "a NULL field must not null the key"
    assert hashes[0] == hashes[1], "the same poison record twice must hash the same"
    assert hashes[0] != hashes[2], "a different amount is a different record"


def test_a_keyed_row_stays_distinct_from_a_redelivery(spark):
    """The key must not collapse two genuine deliveries into one DLQ row."""
    from pyspark.sql.types import LongType, StringType, StructField, StructType

    schema = StructType(
        [StructField(c, LongType() if "paise" in c else StringType()) for c in iw._DLQ_COLS]
    )
    rows = [
        ("t1", 100, "SUCCESS", "2026-06-10T00:00:00", "m1", "r1"),
        ("t1", 100, "SUCCESS", "2026-06-10T00:00:00", "m1", "r2"),
        ("t2", 100, "SUCCESS", "2026-06-10T00:00:00", "m1", "r1"),
    ]
    df = spark.createDataFrame(rows, schema)
    keyed = df.withColumn("payload_hash", iw._dlq_dedupe_hash(iw._DLQ_COLS))
    assert df.dropDuplicates(["transaction_id"]).count() == 2, "t1 collapses on the id"
    assert keyed.dropDuplicates(["payload_hash"]).count() == 3, (
        "a different processing_run_id is a different delivery, and collapsing "
        "them would hide a repeat"
    )


def test_the_hash_covers_every_dlq_column():
    src = inspect.getsource(iw._dlq_dedupe_hash)
    assert "cols" in src
    # If a column is added to _DLQ_COLS and not to the hash, the key silently
    # stops discriminating. Assert the caller passes the whole list.
    caller = inspect.getsource(iw.run_ingestion)
    assert "_dlq_dedupe_hash(_DLQ_COLS)" in caller


def test_the_dlq_rows_are_deduplicated_on_the_key():
    src = inspect.getsource(iw.run_ingestion)
    dlq = src[src.index("MERGE INTO {dlq_table}") :]
    assert "ON t.payload_hash = s.payload_hash" in dlq
    assert "t.transaction_id = s.transaction_id" not in dlq, (
        "the nullable key is what made replays append forever"
    )
    window = src[src.index("inv = batch_df.filter(~F.coalesce(accepted_cond") :]
    assert "payload_hash" in window and "dropDuplicates" in window
    assert 'dropDuplicates(["transaction_id"])' not in window, (
        "the purge can only reach rows that have a transaction_id, so keying "
        "on it hides exactly the rows nobody can clear"
    )


def test_the_dlq_table_carries_and_backfills_the_column():
    src = inspect.getsource(iw.ensure_dlq_table)
    assert "payload_hash string" in src
    assert "ADD COLUMN IF NOT EXISTS payload_hash string" in src


def test_the_webhook_dedup_still_keys_on_the_transaction_id():
    """The change was scoped to the DLQ; the webhook path must be untouched."""

    src = inspect.getsource(iw.run_ingestion)
    assert 'filter(accepted_cond).dropDuplicates(["transaction_id"])' in src
