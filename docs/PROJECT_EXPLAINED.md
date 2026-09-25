# tx-recon, explained for non-technical readers

## The problem, in one sentence

When you pay a shop online, two records are created — the payment app's
instant notification and the bank's next-day statement — and someone has to
check that they agree.

## Why that checking matters

Every online payment carries a small fee (called MDR) plus tax on that fee.
The bank's statement arrives a day late, and its fee math doesn't always match
the payment app's. Today, finance teams check this by hand in spreadsheets.
That is slow (month-end closing drags on for days) and it hides lost money:
if the bank takes a slightly bigger fee than it should, nobody notices.

## What this project does

tx-recon is a small automated system that does the matching for them:

1. **Listens.** Payment notifications arrive in real time and are collected
   in order (like a post office sorting incoming letters) into a bucket-partitioned Iceberg table so later matches scan only the relevant file groups.
2. **Receives the settlement file.** The next day, the PG's statement arrives as
   a spreadsheet-like CSV. Each file is tagged to its provider batch (`razorpay`/`cashfree`/`payu` from `config/providers.yaml`) — not just a calendar date — so cross-midnight cut-offs and holidays don't create false "missing" cases.
3. **Checks the file first.** Before trusting any file the system: scans for card numbers (so a leaked PAN never enters the lake), records the file's fingerprint so the same file dropped twice is recognised, then inspects columns — correct types, real calendar dates, no PAN. Bad rows are set aside in a "quarantine" pile for a human to look at, never silently thrown away. Good rows are saved as a `curated` copy; the quarantined rows never reach the next step.
4. **Publishes via a branch.** The day's settlement is first written to an isolated branch (`ingest/YYYY-MM-DD`). Only after the checks pass is that branch merged to the main table — if the checks fail, the branch is dropped and the main table is untouched.
5. **Recalculates every fee from scratch.** Using a published, versioned rate card (fee percentage per payment method + 18% tax + per-merchant overrides + effective dates), it recomputes what each payment's fee should have been — using whole paise only, never decimals, so rounding can never corrupt money math. The same arithmetic runs in Python and in Spark SQL.
6. **Matches (three legs).** Each payment is compared against its PG record: exact match, tiny rounding difference (accepted within 1 paise), fee disagreement, or missing on one side. Then an independent bank-statement check (MT940 parsed via `mt-940`, never hand-rolled) looks for a matching bank credit — a gateway-consistent error is invisible without this third leg.
7. **Repeats safely.** The whole process can be re-run any number of times
   without double-counting — late corrections simply update the previous answer, and every manual fix is logged append-only with two sets of eyes.

## How it proves it's right

Speed means nothing if the matches are wrong, so correctness is tested first:

- The 10k real sample (`data/samples/real_10k_*.csv`) is joined through
  `FeeEngine.check_match` with a secret nothing: the loader mix targets ~90%
  matched, so anything below 85% means fee miscalibration or drift.
- Latest result: **94.6% match** (8813 matched / 500 mismatched / 492 orphans
  on 9805 settlement ids) — gate passes, orphans visible.

## How fast is it (measured 25 September 2026, ordinary desktop, single node)

- **End-to-end batch (12.6M rows, real thiru card data, 15.8 GB RAM, 8g driver):** ~5 minutes wall — 173 s validation (12.9M clean) + 105 s MERGE + mart (89.98% match on the injected ~10% exception mix, health gate ≥ 85%).
- **MERGE slices (real data, 128 shuffles ≥5M):** 1M 50% 13.25 s, 5M 50% 11.61 s, **12M 50% 56.73 s** (slices show ~94.7% due to tx-ordered sampling; see `docs/REAL_DATA.md`).
- **File checking (12.9M rows, same box):** polars 9.43M / manual 3.34M / pandera 763k / pydantic 178k rows/s (`results_pandera_real.json`).
- **Queue (real ~150B Avro, acks=all, lz4, Redpanda):** 225,123 msgs/sec, serial p50 0.65 ms / p95 1.11 ms / p99 3.97 ms.
- All suites run on real data; `docs/BENCHMARKS.md` holds the full method and the real-data suite is the cited scale proof.

## Honest limits

- This is a **demonstration**, not a production system: single-node, ~13M real-notional card rows (USD magnitudes as notional paise) proven, but not billions; streaming exactly-once is checkpoint + idempotent MERGE, not transactional outbox.
- Fee math is INR-only by ledger design; real cross-currency FX would need a second schedule.
- Cross-region, KMS-at-rest beyond bucket lifecycle, and full Trino RBAC remain Terraform stubs (`infra/terraform/main.tf` is still S3+Glue+skeleton).
- Anything above marked "pending" is genuinely not done — the README and REAL_DATA numbers are updated only with measurements actually taken.

## Seeing it work

With Docker running: `docker compose up -d` starts the supporting services,
`make accuracy` runs the 10-second real-data correctness gate (match_rate >= 85% on the 10k sample), and
`python -m src.pipeline --date 2026-09-04` runs the real PG file path (fail-closed, never synthesized).
