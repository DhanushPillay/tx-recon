"""One parse for `timestamp_utc`, and the fourth parser this removed.

`timestamp_utc` is an unconstrained Avro string. Four expressions were reading
it: a bare `to_date` in the late-SLA sweep, the same bare `to_date` again in the
lag count, `to_timestamp` in ingestion, and `TRY_CAST` in the mart DDL. Every
shape one of them rejects becomes NULL.

That matters more than it looks. A NULL timestamp is never aged to
`EXCEPTION_LATE_UNRESOLVED` and never counted in `missing_within_lag`, which is
the denominator of the batch match rate. So a batch full of unparseable
timestamps scored *better* than a clean one.

These tests pin the measured behaviour rather than an assumption about it. The
accepted and rejected lists below are what Spark 3.5 actually does, established
by running every variant over every shape -- see the note on the pattern in
`reconcile.webhook_timestamp_sql`.
"""

from __future__ import annotations

import inspect

from src.processing import reconcile as rc

# What Spark 3.5's default timestamp parser takes. Fractional seconds of any
# length, Z or a numeric offset, and the space-separated and date-only forms.
ACCEPTED = [
    "2026-06-10T00:00:00",
    "2026-06-10T14:30:05",
    "2026-06-10T14:30:05.1",
    "2026-06-10T14:30:05.123",
    "2026-06-10T14:30:05.1234",
    "2026-06-10T14:30:05.123456",
    "2026-06-10T14:30:05.123456789",
    "2026-06-10T14:30:05Z",
    "2026-06-10T14:30:05.500Z",
    "2026-06-10T14:30:05+05:30",
    "2026-06-10T14:30:05.250-04:00",
    "2026-06-10 14:30:05",
    "2026-06-10",
]

# Shapes with no defensible instant. These have to come back NULL, and that has
# to be counted rather than swallowed.
REJECTED = [
    "10/06/2026",
    "not-a-timestamp",
    "",
    "2026-13-45T99:99:99",
]

# An explicit pattern is *stricter* than the default parser, and the pattern this
# bug invited -- yyyy-MM-dd'T'HH:mm:ss[.SSS][XXX] -- rejects four of the shapes
# above. Pinning it would have broken rows that parse today.
WOULD_BREAK = [
    "2026-06-10T14:30:05.1234",
    "2026-06-10T14:30:05.123456",
    "2026-06-10 14:30:05",
    "2026-06-10",
]


def _parsed(spark, exprs: list[str]) -> dict[str, object]:
    from pyspark.sql.functions import col, expr

    frame = spark.createDataFrame([(v,) for v in ACCEPTED + REJECTED], "ts string")
    selected = frame.select(col("ts"), *[expr(e).alias(f"c{i}") for i, e in enumerate(exprs)])
    return {r["ts"]: [r[f"c{i}"] for i in range(len(exprs))] for r in selected.collect()}


def test_the_helpers_emit_one_permissive_parse(spark):
    assert rc.webhook_timestamp_sql("t.timestamp_utc") == "to_timestamp(t.timestamp_utc)"
    assert rc.webhook_date_sql("x") == "to_date(to_timestamp(x))"
    assert rc.webhook_date_sql() == rc.webhook_date_sql("timestamp_utc")


def test_every_site_now_agrees(spark):
    """One parse, checked by running all four expressions over one frame."""
    sites = {
        "helper_timestamp": rc.webhook_timestamp_sql("ts"),
        "helper_date": rc.webhook_date_sql("ts"),
        "ingest_event_time": "to_timestamp(ts)",
        "mart_transacted_at": "cast(ts as timestamp)",
    }
    got = _parsed(spark, list(sites.values()))

    for value, cells in got.items():
        for name, parsed in zip(sites, cells, strict=True):
            if value in ACCEPTED:
                assert parsed is not None, f"{name} rejected {value!r}, which parses today"
            else:
                assert parsed is None, f"{name} turned {value!r} into {parsed!r}"

    # ...and they really do agree, row for row. The date helper is deliberately
    # not one of them: it is the date part of the instant, so it agrees with the
    # others on the calendar day and nothing finer.
    instants = {k: v for k, v in sites.items() if k != "helper_date"}
    for value, cells in got.items():
        stamps = [c for k, c in zip(sites, cells, strict=True) if k in instants]
        date = next(c for k, c in zip(sites, cells, strict=True) if k == "helper_date")
        if value not in ACCEPTED:
            continue
        assert len(set(stamps)) == 1, (
            f"{value!r} reads differently per instant site: "
            f"{ {k: c for k, c in zip(instants, stamps, strict=True)} }"
        )
        assert date == stamps[0].date(), (
            f"{value!r}: the date helper gave {date!r} for an instant of {stamps[0]!r}"
        )


def test_an_explicit_pattern_would_have_been_worse(spark):
    """Why there is no pattern, as an executable claim rather than a comment."""
    from pyspark.sql.functions import col, expr, to_timestamp

    strict = "yyyy-MM-dd'T'HH:mm:ss[.SSS][XXX]"
    loose = rc.webhook_timestamp_sql("ts")
    frame = spark.createDataFrame([(v,) for v in WOULD_BREAK], "ts string")

    strict_out = {
        r["ts"]: r["o"]
        for r in frame.select("ts", to_timestamp(col("ts"), strict).alias("o")).collect()
    }
    loose_out = {r["ts"]: r["o"] for r in frame.select("ts", expr(loose).alias("o")).collect()}

    for value in WOULD_BREAK:
        assert loose_out[value] is not None, f"{value!r} must keep parsing"
        assert strict_out[value] is None, (
            f"{value!r} is in WOULD_BREAK because the explicit pattern rejects it; "
            "if this now parses, the pattern is no longer stricter and the "
            "docstring in reconcile.webhook_timestamp_sql is out of date"
        )


def test_no_call_site_still_parses_it_its_way():
    """A bare `to_date(t.timestamp_utc)` reappearing is the drift we are fixing."""
    reconcile_src = inspect.getsource(rc)
    assert "to_date(t.timestamp_utc)" not in reconcile_src
    assert "to_date(timestamp_utc)" not in reconcile_src
    assert "TRY_CAST(timestamp_utc" not in reconcile_src
    assert "webhook_date_sql('t.timestamp_utc')" in reconcile_src


def test_the_mart_parses_it_the_same_way(tmp_path):
    """The mart shows operators the same instant the gates reasoned about."""
    body, version = rc._load_mart_sql(
        str(tmp_path), "nessie.db.webhooks", "nessie.db.fact_reconciliation"
    )
    for placeholder in ("__SOURCE_TABLE__", "__MART_TABLE__", "__TIMESTAMP_UTC__"):
        assert placeholder not in body, f"{placeholder} was not substituted"
    assert rc.webhook_timestamp_sql("timestamp_utc") in body
    assert "TRY_CAST" not in body, "TRY_CAST is the parser this removed"
    assert version


def test_the_shipped_mart_migration_uses_the_placeholder():
    """The checked-in SQL carries the placeholder, not a hardcoded parse."""
    import os

    from src.common.settings import get_settings

    path = os.path.join(
        get_settings().project_root, "sql", "marts", "migrations", "V1__fact_reconciliation.sql"
    )
    with open(path, encoding="utf-8") as fh:
        body = fh.read()
    assert "__TIMESTAMP_UTC__ AS transacted_at" in body
    assert "TRY_CAST" not in body


def test_the_inline_mart_fallback_agrees_too(tmp_path):
    """The fallback path exists for a missing migration file; same parse."""
    empty = tmp_path / "no-migrations-here"
    empty.mkdir()
    body, version = rc._load_mart_sql(str(empty), "db.webhooks", "db.fact")
    assert version == "inline"
    assert rc.webhook_timestamp_sql("timestamp_utc") in body
    assert "TRY_CAST" not in body
