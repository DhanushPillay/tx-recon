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
  placeholders. Never expose these ports beyond `localhost`.
- Secret scanning runs in CI (`gitleaks/gitleaks-action`, detect on the
  working tree). No image scanning yet (tracked as future work).
- `src/processing/residual.py` interpolates transaction IDs into SQL;
  acceptable for a local demo, must be parameterized before any shared
  deployment.
- PII (`transaction_id`, `merchant_id`, amounts) sits cleartext in
  `data/settlement_*.csv`, quarantine CSVs, and the Iceberg DLQ. Local demo
  only: encrypt the volume/bucket, purge quarantine + DLQ on a schedule
  (`python scripts/replay_dlq.py --delete` after replay), mask IDs in logs
  and dashboards before any shared deployment.
- Real-data provenance: `thiru1711/Financial_Transactions` (Hugging Face) has
  no license card — used on maintainer's explicit acceptance; re-evaluate
  before any redistribution. PII (`card_number`, names, addresses, balances)
  is stripped by the one-time clean step (`data/thiru_clean.parquet`) and by
  `src/adapters/real_data.py`; only tx id, amount, date, merchant, card type
  enter the pipeline. Amounts are USD magnitudes treated as notional paise
  (ledger stays INR-only via NULL currency).
- Deps are range-pinned (`pyproject.toml`); reproduce exact builds with
  `uv lock` and run `pip-audit` (CI already gates on it).
- Webhook authenticity: producer signs `x-tx-sig: HMAC(transaction_id|amount)`
  when `WEBHOOK_SECRET` is set; broker trust comes from SASL
  (`KAFKA_SECURITY_PROTOCOL` + `KAFKA_SASL_*`). Spark's Kafka source exposes
  no headers, so HMAC verify lives at the gateway edge, not in
  `ingest_webhooks.py` (which warns when the secret is set without SASL).
