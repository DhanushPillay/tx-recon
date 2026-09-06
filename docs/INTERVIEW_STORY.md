# INTERVIEW_STORY — the 90-second version

> Open with: "Payment webhooks arrive in real time, bank settlements arrive a
> day late and disagree about fees. tx-recon matches them and proves every
> match is right."

## The script

1. **Grain.** One row per `transaction_id`. Webhooks stream in via Redpanda;
   settlements land as CSVs. Grain is the contract everything else hangs off.
2. **Fees.** MDR + 18% GST computed in integer paise — no floats, ever. One
   canonical `FeeEngine`; the Spark SQL in the MERGE is generated from it, and
   a golden test pins SQL == Python across instruments and edge amounts.
3. **Match.** A single idempotent Iceberg `MERGE`: tolerance-aware
   (`|expected_net − settled| ≤ tolerance` ⇒ MATCHED), re-runnable with no
   double-count, late corrections converge because matched rows UPDATE.
4. **Prove it.** A sealed harness injects 7 break classes (exact, rounding,
   fee mismatch, orphan, duplicate, out-of-order, late correction) with an
   answer key the matcher never sees: P=R=F1=1.0, zero false positives.
   `make demo` runs it in ~10 seconds, no infra.

## Live demo (clean checkout, <5 min with docker)

```bash
docker compose up -d          # Redpanda + MinIO + Nessie
make demo                     # quick gate: F1=1.0 + hardware fingerprint
python -m src.pipeline --date 2026-09-04   # generate → validate → reconcile
```

## Questions you'll get

- *Why Iceberg MERGE over joins?* ACID upsert on the grain; re-runs converge
  instead of duplicating. The MERGE-shape test asserts 2×UPDATE / 1×INSERT /
  no DELETE.
- *Why integer paise?* Floats round; money must not. SQL uses integer `DIV`
  mirroring the engine exactly.
- *Why both Pandera and quarantine?* Write-Audit-Publish: contracts validate
  at the batch boundary, failures isolate to `quarantine_*.csv` / DLQ instead
  of crashing the pipeline.
- *How do you know a slowdown is real?* Baselines carry a hardware
  fingerprint; the regression gate warns on a new machine and only fails on
  metric movement (>15% throughput drop, >20% p99 rise).
- *What doesn't it do?* Single-node, synthetic data, no exactly-once Kafka
  semantics — the honest scope is in the README, not the demo.
