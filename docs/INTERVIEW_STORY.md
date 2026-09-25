# INTERVIEW_STORY — the 90-second version

> Open with: "Payment webhooks arrive in real time, bank settlements arrive a
> day late and disagree about fees, and gateways can be consistently wrong. tx-recon matches gateway vs bank on three legs and proves every match is right."

## The script

1. **Grain.** One row per `transaction_id`, bucketed `bucket(16, transaction_id)` so MERGE prunes to file groups. Webhooks stream in via Redpanda; settlements land as CSVs tagged to provider batches from `config/providers.yaml`. Grain + bucket + batch is the contract everything else hangs off.
2. **Fees.** MDR + 18% GST computed in integer paise — no floats, ever. Versioned rate cards (`effective_from/to`, per-merchant overrides). One canonical `FeeEngine`; the Spark SQL in the MERGE is generated from it (`build_fee_case_sql`), and a golden test pins SQL == Python across instruments, merchants, and edge amounts.
3. **Match.** A single idempotent Iceberg `MERGE` (4×UPDATE / 1×INSERT / no DELETE, see RUNBOOK): preserves `MISSING_WEBHOOK` placeholders and `LATE_UNRESOLVED` terminal rows, then tolerance-aware (`|expected_net − settled| ≤ tolerance` ⇒ MATCHED) — re-runnable, late corrections converge because matched rows UPDATE. Batches land on a WAP branch (`ingest/YYYY-MM-DD`, `write.wap.enabled` + `spark.wap.branch` both required) and merge to `main` only on gate pass; per-provider `lag_days`/`late_sla_days` drive `missing_within_lag` and late-SLA marking.
4. **Third leg.** Matches alone can be gateway-consistent yet wrong. `src/adapters/bank_statement.py` parses MT940 via `mt-940` into `bank_statements`; `build_bank_leg_sql` demotes MATCHED without independent bank credit to `EXCEPTION_MISSING_BANK_STATEMENT`.
5. **Prove it.** The 10k real sample joins through `FeeEngine.check_match`:
   94.6% match with orphans visible, gated at >= 85% — a drop means fee
   miscalibration or drift, not a bad seed.
   `make accuracy` runs it in ~10 seconds, no infra. Real thiru scale (12.6M, 89.98% on ~10% mix, 91.7% coverage) proves it at size.

## Live demo (clean checkout, <5 min with docker)

```bash
docker compose up -d          # Redpanda + MinIO + Nessie
make accuracy                 # real-data gate: match_rate >= 85% on 10k sample + hardware fingerprint
python -m src.pipeline --date 2026-09-04               # real PG file (fail-closed, no synthesis)
```

## Questions you'll get

- *Why Iceberg MERGE over joins?* ACID upsert on the grain + bucket pruning; re-runs converge instead of duplicating. The MERGE-shape test asserts **4×UPDATE / 1×INSERT / no DELETE** (MISSING preserve, LATE preserve, null-guard, fee check) and the maintenance order test pins `expire -> orphan 3d -> binpack -> manifests`.
- *Why integer paise?* Floats round; money must not. SQL uses integer `DIV` mirroring the engine exactly (`(a*b+5000)//10000` half-up).
- *Why both Pandera and quarantine?* WAP: contracts validate on the branch, failures isolate to `quarantine_*.csv` / DLQ and the branch is dropped — they never reach `main`. Add PAN Luhn gate + file `sha256` registry: same bytes twice is a redelivery, not double count.
- *Why a bank third leg?* Gateway pair can be consistently wrong. MT940 via `mt-940` provides independent credit evidence; without it `EXCEPTION_MISSING_BANK_STATEMENT` is invisible.
- *Why provider batches?* Calendar date creates false MISSINGs around cut-offs. `config/providers.yaml` per-provider `lag_days`/`late_sla_days` + `missing_within_lag` gauge separates expected delay from truly missing.
- *How do you know a slowdown is real?* Baselines carry a hardware fingerprint; the regression gate warns on a new machine and only fails on metric movement (>15% throughput drop, >20% p99 rise, >15% rows/sec drop, match_rate < 85%). Every benchmark runs on real data (`results_real.json`).
- *What doesn't it do?* Single-node (13M proven, not billions), USD-magnitudes as notional INR, no transactional outbox — checkpoint + idempotent MERGE + bucket layout is the scale story. Full prod Terraform (KMS, per-table lifecycle, IAM) remains a skeleton.
