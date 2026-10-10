# Proof: Downgrade-Only Residual Cannot Increase False Positives

## Claim

Let `M` be the base MERGE matcher (rule-based, instrument and merchant aware) and `R_tau` the residual post-pass that may demote `MATCHED -> EXCEPTION_FEE_MISMATCH` when `score_match >= tau`, never the reverse. Then for any ledger `L` and threshold `tau`:

```
FP(R_tau(M(L))) <= FP(M(L))
```

where `FP` is hallucinated `MATCHED` (pred==MATCHED while the fee math says mismatch), as exercised in `tests/processing/test_residual.py`.

## Proof

Partition predictions of `M`:

- `A = {x | M(x)=MATCHED and truth(x)=MATCHED}` (true positives)
- `B = {x | M(x)=MATCHED and truth(x)!=MATCHED}` (false positives)
- `C = {x | M(x)!=MATCHED}` (already not matched; residual not invoked)

`R_tau` acts only on `A union B`. For each `x` in `A union B`, it either keeps `MATCHED` or maps to `EXCEPTION_FEE_MISMATCH`. No `x` in `C` is mapped to `MATCHED` (no promotion). Hence:

1. Every `x` in `B` that `R_tau` demotes leaves `B` (FP decreases by one).
2. No `x` in `C` enters `B` (no new FP).
3. Every `x` in `A` that `R_tau` demotes becomes a false negative, not a false positive. Recall may drop, FP does not rise.

Therefore `FP` after `R_tau` is a subset of `B`, monotone non-increasing.

## Calibration for this repo

- Harness gate: `match_rate >= 85%` on the 10k real sample (`tests/performance/test_recon_accuracy.py`, measured 94.6%).
- Real-data expectation (different gate): the thiru loader mix is ~10%
  exceptions by design ([thiru1711/Financial_Transactions](https://huggingface.co/datasets/thiru1711/Financial_Transactions)),
  so production MERGE targets match rate ≥ 85%, not F1=1.0. Measured 89.98%
  end-to-end (12.6M rows) and ~94.7% on tx-ordered bench slices. A real-data
  89–95% is the mix working, not the matcher failing — compare against the
  loader's known buckets, never against 1.0.
- Base `M` already achieves `FP==0` on the property draws. With `DEFAULT_TAU=0.9`, `score_match==0.95` only on merchant-rate disagreement (default says MATCH, merchant says MISMATCH). No `A` row is demoted, so `R_tau` is identity on the harness. The invariant `FP(R_tau(M))==0` holds.
- If merchant rates drift in production, `R_tau` can recover FP that a default-only MERGE would introduce, at the cost of possible recall loss. FP never increases.

## Hypothesis property

Checked in `tests/processing/test_residual.py`:

```python
@given(amount, instrument, merchant, settled)
def test_downgrade_never_creates_fp():
    pred_before = base_match(...)
    pred_after = downgrade_only(pred_before, score, tau)
    assert fp(pred_after) <= fp(pred_before)
```

The test draws random ledgers via hypothesis, scores each one through the **SQL the residual pass actually runs** (`score_match_sql`, evaluated by a real Spark session) and through `score_match`, asserts the two agree, then asserts the inequality over the SQL-derived score. It also asserts the pass can only demote: `apply_residual`'s MERGE has no `WHEN NOT MATCHED`, its source is gated on `reconciliation_status = 'MATCHED'`, and its `UPDATE SET` writes only `EXCEPTION_FEE_MISMATCH`.

So a failure means one of three things: the SQL and Python scorers disagree, the score is non-monotone, or the demote-only gate was widened.

## What is not proven

- Recall may decrease (intentional). The proof does not claim `F1` or `recall` preservation, only `FP` monotonicity.
- That `apply_residual`'s demotion cannot later be undone by the next reconciliation batch: `EXCEPTION_FEE_MISMATCH` is not in `TERMINAL_STATUSES`, so clause 3 re-evaluates it and a corrected settlement promotes it back to `MATCHED`. That is intended (a corrected settlement should win) but it means the residual verdict is not sticky.
- Timing, liveness, and streaming dedup are not covered here; they are tested separately in `docs/RUNBOOK.md` failure injection scenarios.
