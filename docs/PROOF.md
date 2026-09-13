# Proof: Downgrade-Only Residual Cannot Increase False Positives

## Claim

Let `M` be the base MERGE matcher (rule-based, instrument and merchant aware) and `R_tau` the residual post-pass that may demote `MATCHED -> EXCEPTION_FEE_MISMATCH` when `score_match > tau`, never the reverse. Then for any ledger `L` and threshold `tau`:

```
FP(R_tau(M(L))) <= FP(M(L))
```

where `FP` is hallucinated `MATCHED` as defined in `tests/performance/recon_accuracy.py` (`pred==MATCHED` while `truth!=MATCHED`).

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

- Harness gate: `F1==1.0, FP==0, fanout==1` on 2000 rows x 3 seeds (`tests/performance/test_recon_accuracy.py`).
- Base `M` already achieves `FP==0`. With `DEFAULT_TAU=0.9`, `score_match==0.95` only on merchant-rate disagreement (default says MATCH, merchant says MISMATCH). No `A` row is demoted, so `R_tau` is identity on the harness. The invariant `FP(R_tau(M))==0` holds.
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

The test constructs random ledgers via the sealed harness and asserts the inequality for all draws. It fails if any code path promotes `MISMATCH -> MATCHED`.

## What is not proven

- Recall may decrease (intentional). The proof does not claim `F1` or `recall` preservation, only `FP` monotonicity.
- Timing, liveness, and streaming dedup are not covered here; they are tested separately in `docs/RUNBOOK.md` failure injection scenarios.
