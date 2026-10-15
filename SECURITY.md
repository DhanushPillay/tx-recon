# Security Policy

## Supported Versions

| Version | Supported          |
| ------- | ------------------ |
| main    | :white_check_mark: |
| < 0.1.0 | :x:                |

This is a local-first stack (MinIO, Redpanda, Nessie all run on
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
  placeholders, and every published port is bound to `127.0.0.1` — in
  `docker-compose.yml` and in `docker-compose.localstack.yml` — rather than
  `0.0.0.0`. That binding is what keeps the unauthenticated services (Trino,
  Nessie, Redpanda admin, Metabase, and LocalStack's S3/IAM/STS endpoints) off
  the network, so do not "simplify" a `"127.0.0.1:PORT:PORT"` mapping back to
  `"PORT:PORT"`. LocalStack is the sharpest edge here: it mints IAM and STS
  credentials with no authentication at all, so anything that can reach it can
  mint credentials. Secrets support
  `*_FILE` (mounted-file) resolution (`MINIO_SECRET_KEY_FILE`, `WEBHOOK_SECRET_FILE`,
  `KAFKA_SASL_PASSWORD_FILE`: file wins over empty env, explicit env wins over file).
  Prod must set `REQUIRE_KAFKA_SASL=1` (PLAINTEXT brokers trust forgeable records),
  `REQUIRE_SCHEMA_REGISTRY=1` when drift must fail closed, and keep `ALLOW_DESTRUCTIVE_SEED`
  unset outside explicitly confirmed seed runs (real-data seeding is `MERGE-DELETE`). `strict_slo=true`
  is the default (sub-SLO, stale mart, >50% volume drop, >48h-old file fail the batch).
  `STRICT_FEE_RATES` defaults to `false`: a settlement dated outside the rate
  history is normal on real data, so it is counted and warned about rather than
  failed. Turn it on where a missing rate should stop the batch.
- Secret scanning runs in CI (`gitleaks/gitleaks-action`, detect on the
  working tree). No image scanning yet (tracked as future work).
- `src/processing/residual.py` interpolates transaction IDs into SQL;
  acceptable for a local stack, must be parameterized before any shared
  deployment. `src/processing/reconcile.py: _qualified_table` guards table identifiers; residual SQL is the remaining interpolation surface.
- PII (`transaction_id`, `merchant_id`, amounts) sits cleartext in
  `data/settlement_*.csv`, `curated_*.csv`, quarantine CSVs, and the Iceberg DLQ — but the batch boundary now scans for PAN (`src/validation/pan_guard.py`, Luhn-verified, fail-closed) and records `sha256` per file in `data/.processed_files.json`. Local stack only: encrypt the volume/bucket, purge quarantine + DLQ on a schedule (`python scripts/replay_dlq.py --reason ... --approved-by ... --delete` journals to `data/corrections.jsonl`), mask IDs in logs and dashboards before any shared deployment. Card numbers never enter the pipeline: last-4 + issuer only, verified by the one-time clean step.
- Real-data provenance: `thiru1711/Financial_Transactions` (Hugging Face) has
  no license card — used on maintainer's explicit acceptance; re-evaluate
  before any redistribution. PII (`card_number`, names, addresses, balances)
  is stripped by the one-time clean step (intermediate parquet, since removed) and by
  `src/adapters/real_data.py`; only tx id, amount, date, merchant, card type
  enter the pipeline. Amounts are USD magnitudes treated as notional paise
  (ledger stays INR-only via NULL currency). Provenance is recorded in this file.
- Deps are range-pinned (`pyproject.toml`); reproduce exact builds with
  `uv lock` and run `pip-audit`. CI installs from the lock (`uv export
  --frozen`) rather than re-resolving from PyPI, and gates blocking on
  `pip-audit` with 97 ignores: 96 in `.github/pip-audit-ignore.txt` plus one
  inline in `ci.yml`. Each carries a one-line reason, and the file's
  `REVIEW_BY` date is enforced by a CI step that fails the build once it
  passes. The transitive floors under `apache-airflow` (PyJWT, Werkzeug,
  urllib3, multidict, fsspec) are declared in `pyproject.toml` rather than
  ignored, so airflow's next minor bump picks the fixes up on its own.
  Override rules live in `ci.yml` and the baseline file, not in docs.
- Corrections are append-only with maker-checker (`src/processing/corrections.py: journal_correction`
  requires `reason` + `approved_by`; `scripts/replay_dlq.py` requires the same flags and journals the purge). No mutation without two eyes.

## Webhook authenticity

**Nothing in this repository authenticates a webhook as it flows today.**
`ingest_webhooks.py` reads a Kafka topic with Spark's structured streaming
source, which surfaces record values and no headers, so there is nowhere in
the ingestion path to check a signature even if the producer sent one.

`src/ingestion/webhook_signing.py` is the verification half of a contract the
producer has to meet. It is a library with no listener and no wiring: nothing
imports it, and two tests assert that stays true (`test_it_does_not_listen_on_anything`,
`test_the_module_docstring_does_not_claim_it_is_wired_into_ingestion`).

The contract it implements, for a gateway that terminates HTTP and republishes
to Kafka:

- the signature header is `x-tx-sig: t=<unix seconds>,v1=<hex>`;
- the HMAC-SHA256 key is `WEBHOOK_SECRET`, compared with `hmac.compare_digest`
  over the raw bytes — no string comparison, no early exit between versions;
- the signed payload is `<timestamp>.<raw body>`, the timestamp separated from
  the body by a dot so the boundary cannot be shifted;
- `x-tx-id` carries the delivery id, which the replay cache keys on;
- a signature older or newer than `WEBHOOK_SIGNATURE_TOLERANCE_SEC`
  (300s) is refused as `EXPIRED` before the replay cache is consulted;
- a delivery id seen inside the cache TTL (600s, 10,000 entries, both bounded)
  is refused as `REPLAYED`.

`verify()` returns a reason for every rejection and raises for none of them, so
a caller cannot forget to handle one. Missing configuration reports
`NOT_CONFIGURED` rather than quietly accepting everything, which matters: a
gateway that starts signing and an adapter that forgets to verify should be
loud, not silent.

To close the gap for real, run a verifying gateway in front of the topic and
treat `webhooks` as trusted input. Until then, the only control on the wire is
`REQUIRE_KAFKA_SASL=1`, which is off by default. `ingest_webhooks.py` logs a
warning whenever `WEBHOOK_SECRET` is set so the gap stays visible at run time.

## Transport

No TLS anywhere. Kafka, MinIO, Nessie and Trino all speak plaintext, and the
`Dockerfile.airflow` image inherits Airflow's Fernet-encrypted connections as
the only credential-at-rest story. Everything binds to `127.0.0.1`, which keeps
plaintext off the network locally and does nothing for a shared deployment.
`certifi` is a declared dependency that no module imports; it is an Airflow
transitive, not a trust store anything here consults. Before anything shared,
terminate TLS at the ingress and require `REQUIRE_KAFKA_SASL=1`.

## Retention

There is no retention policy, TTL, or deletion schedule in this repository.
Every one of these grows until something removes it:

| What | Where | Removed by |
| --- | --- | --- |
| Settlements, quarantined rows | `data/*.csv` | nothing, manual delete |
| Webhooks, DLQ rows | Iceberg `webhooks`, `webhooks_dlq` | `scripts/replay_dlq.py --delete`, manual |
| Mart `fact_reconciliation` | Iceberg | `reconcile.refresh_mart` replaces it wholesale |
| Iceberg snapshots and data files | S3 | `maintain_tables` expires snapshots, keeps data |
| Correction journal | `data/corrections.jsonl` | nothing, append-only by design |

`docs/ARCHITECTURE.md` calls the `ingest/YYYY-MM-DD` branch a 3-day TTL. That
is the branch reference's lifetime, not a data deletion policy, and nothing in
the code expires those branches.

Purge the DLQ deliberately, not reflexively: `--delete` requires `--reason` and
`--approved-by` and journals the purge to `data/corrections.jsonl`.
`replay_dlq.py` also reports a `reversal_stranded` count — rows that arrived
before refund handling existed, that no replay predicate matches. They cannot
be replayed or bulk-purged, so they need a human.
