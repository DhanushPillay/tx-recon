# Security Policy

## Supported Versions

| Version | Supported          |
| ------- | ------------------ |
| main    | :white_check_mark: |
| < 0.1.0 | :x:                |

This is a local-first demo stack (MinIO, Redpanda, Nessie all run on
`localhost` with placeholder credentials). It is not hardened for production
use, and the threat model below reflects that.

## Reporting a Vulnerability

Use **GitHub Private Vulnerability Reporting** (Security tab →
Report a vulnerability). Do not open a public issue for a suspected
vulnerability.

What to include: affected file/commit, reproduction steps, and impact
assessment. Expect an initial response within 7 days.

## Known Non-Goals (by design, not oversight)

- Default credentials in `docker-compose.yml` / `.env.example` are local-only
  placeholders. Never expose these ports beyond `localhost`. Secrets support
  `*_FILE` (mounted-file) resolution (`MINIO_SECRET_KEY_FILE`, `WEBHOOK_SECRET_FILE`,
  `KAFKA_SASL_PASSWORD_FILE`: file wins over empty env, explicit env wins over file).
  Prod must set `REQUIRE_KAFKA_SASL=1` (PLAINTEXT brokers trust forgeable records),
  `REQUIRE_SCHEMA_REGISTRY=1` when drift must fail closed, and keep `ALLOW_DESTRUCTIVE_SEED`
  unset outside explicitly confirmed seed runs (demo seeding is `MERGE-DELETE`). `strict_slo=true`
  is the default (sub-SLO, stale mart, >50% volume drop, >48h-old file fail the batch).
- Secret scanning runs in CI (`gitleaks/gitleaks-action`, detect on the
  working tree). No image scanning yet (tracked as future work).
- `src/processing/residual.py` interpolates transaction IDs into SQL;
  acceptable for a local demo, must be parameterized before any shared
  deployment. `src/processing/reconcile.py: _qualified_table` guards table identifiers; residual SQL is the remaining interpolation surface.
- PII (`transaction_id`, `merchant_id`, amounts) sits cleartext in
  `data/settlement_*.csv`, `curated_*.csv`, quarantine CSVs, and the Iceberg DLQ — but the batch boundary now scans for PAN (`src/validation/pan_guard.py`, Luhn-verified, fail-closed) and records `sha256` per file in `data/.processed_files.json`. Local demo only: encrypt the volume/bucket, purge quarantine + DLQ on a schedule (`python scripts/replay_dlq.py --reason ... --approved-by ... --delete` journals to `data/corrections.jsonl`), mask IDs in logs and dashboards before any shared deployment. Card numbers never enter the pipeline: last-4 + issuer only, verified by the one-time clean step.
- Real-data provenance: `thiru1711/Financial_Transactions` (Hugging Face) has
  no license card — used on maintainer's explicit acceptance; re-evaluate
  before any redistribution. PII (`card_number`, names, addresses, balances)
  is stripped by the one-time clean step (`data/thiru_clean.parquet`) and by
  `src/adapters/real_data.py`; only tx id, amount, date, merchant, card type
  enter the pipeline. Amounts are USD magnitudes treated as notional paise
  (ledger stays INR-only via NULL currency). Provenance is recorded in this file.
- Deps are range-pinned (`pyproject.toml`); reproduce exact builds with
  `uv lock` and run `pip-audit`. CI gates blocking on `pip-audit` with 90 ignores (all pip-only locals — no prod service/network exposure; see `pyproject.toml: [tool.pip-audit]`). Re-audit on `uv lock` bumps; overrides live in `pyproject.toml`, not in docs.
- Corrections are append-only with maker-checker (`src/processing/corrections.py: journal_correction` requires `reason` + `approved_by`; `scripts/replay_dlq.py` requires the same flags and journals the purge). No mutation without two eyes.
- Webhook authenticity: producer signs `x-tx-sig: HMAC(transaction_id|amount)`
  when `WEBHOOK_SECRET` is set; broker trust comes from SASL
  (`KAFKA_SECURITY_PROTOCOL` + `KAFKA_SASL_*`). Spark's Kafka source exposes
  no headers, so HMAC verify lives at the gateway edge, not in
  `ingest_webhooks.py` (which warns when the secret is set without SASL and enforces `REQUIRE_KAFKA_SASL` / `REQUIRE_SCHEMA_REGISTRY` when those flags are set).
