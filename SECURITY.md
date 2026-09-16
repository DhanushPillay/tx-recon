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
- No secret scanning or image scanning in CI yet (tracked as future work).
- `src/processing/residual.py` interpolates transaction IDs into SQL;
  acceptable for a local demo, must be parameterized before any shared
  deployment.
