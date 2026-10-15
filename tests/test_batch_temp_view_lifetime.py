"""The settlement frame has to outlive the MERGE and die after the batch.

``run_reconciliation`` exposes the folded settlement rows as the
``bank_settlements`` temp view and caches the frame behind it.
``apply_residual`` reads that view afterwards, from ``pipeline.run_daily``.

That ordering is load-bearing and nothing enforced it. The frame was
unpersisted inside ``run_reconciliation``'s ``finally``, so by the time the
residual ran Spark had no cached copy and re-read the CSV from disk. If the
settlement file changed between the two, the MERGE compared one set of numbers
and the residual scored a different one -- and the residual demotes rows, so it
changes reconciliation outcomes on the basis of a file the batch had already
read once.

The views were also never dropped, so they outlived the batch and the next run's
``createOrReplaceTempView`` shadowed them rather than replacing them.

None of this needs Spark to pin. What is asserted here is the shape: who calls
the release, and where in the sequence it sits relative to the residual.
"""

from __future__ import annotations

import inspect
from unittest.mock import MagicMock

from src.pipeline import run_daily
from src.processing import reconcile as rc


def _source() -> str:
    return inspect.getsource(rc.run_reconciliation)


def test_the_settlement_cache_survives_the_merge_function():
    """The frame stays cached, because something downstream still reads it."""
    body = _source()
    finally_block = body.split("finally:")[-1]
    assert "bank_df_valid.unpersist()" in finally_block, (
        "nothing reads bank_df_valid after the function returns, so it should still be released here"
    )
    assert "bank_df_dedup.unpersist()" not in finally_block, (
        "dropping the cached settlement frame here forces apply_residual to re-read the CSV, "
        "so it scores against whatever the file says now rather than what the MERGE compared"
    )


def test_why_it_stays_is_written_down():
    body = _source()
    assert "apply_residual" in body, (
        "the reason the cache survives belongs next to the code, not only in a test"
    )
    assert "drop_batch_temp_views" in body


def test_the_release_names_both_views_the_batch_creates():
    """A view nobody drops is a view the next run shadows instead of replacing."""
    assert set(rc.BATCH_TEMP_VIEWS) == {"bank_settlements", "bank_statements"}
    assert 'bank_df_dedup.createOrReplaceTempView("bank_settlements")' in _source()
    assert 'createOrReplaceTempView("bank_statements")' in inspect.getsource(rc.reconcile_bank_leg)


def test_the_release_is_safe_when_there_is_nothing_to_release():
    """A cleanup path that raises would turn a good batch into a failed one."""
    spark = MagicMock()
    rc.drop_batch_temp_views(spark)
    assert [c.args[0] for c in spark.catalog.dropTempView.call_args_list] == list(
        rc.BATCH_TEMP_VIEWS
    )
    spark.catalog.clearCache.assert_called_once()


def test_a_failing_release_does_not_mask_the_batches_own_error():
    spark = MagicMock()
    spark.catalog.dropTempView.side_effect = RuntimeError("catalog is gone")
    rc.drop_batch_temp_views(spark)
    assert [c.args[0] for c in spark.catalog.dropTempView.call_args_list] == list(
        rc.BATCH_TEMP_VIEWS
    ), "one dead view must not stop the rest of the cleanup"
    assert spark.catalog.clearCache.called, (
        "the cache is the expensive one to leak, so it is tried even if every view failed"
    )


def test_the_release_runs_last_in_run_daily():
    """After the residual, and after the branch is settled either way."""
    body = inspect.getsource(run_daily)
    assert "drop_batch_temp_views(spark)" in body
    assert body.index("drop_batch_temp_views(spark)") > body.index("_run_residual(counts)"), (
        "releasing the view before the residual pass is the bug this file exists for"
    )
    assert body.index("drop_batch_temp_views(spark)") > body.index("publish_branch("), (
        "the view has to outlive the publish, not be torn down under it"
    )


def test_the_release_is_inside_the_finally():
    """A gate that raises must still give the frame back."""
    body = inspect.getsource(run_daily)
    finally_block = body.split("finally:")[-1]
    assert "drop_batch_temp_views(spark)" in finally_block, (
        "on the rejection path the finally is the only thing that runs"
    )
