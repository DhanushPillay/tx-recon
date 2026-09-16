# Contributing

Solo-maintainer project; contributions welcome via pull request.

## Setup

```bash
cp .env.example .env   # set MINIO_ROOT_PASSWORD
docker compose up -d   # redpanda, minio, nessie, trino, metabase
pip install -e ".[dev]"
python -m src.pipeline --date 2026-09-04
```

Full setup: [docs/LOCAL_SETUP.md](docs/LOCAL_SETUP.md).

## Before opening a PR

```bash
ruff format src/ tests/ dags/ && ruff check src/ tests/ dags/
mypy src/
pytest tests/ -m "not integration" -n auto --benchmark-disable
```

New behavior needs a test. Coverage gate is 90% branch (`--cov-fail-under=90`).

Use [Conventional Commits](https://www.conventionalcommits.org/)
(`feat:`, `fix:`, `docs:`, `test:`, `ci:` …) and fill in
`.github/PULL_REQUEST_TEMPLATE.md`.

## What good PRs look like here

- Small diffs, one concern per PR.
- Integer-paise math stays integer — no floats in fee paths.
- Docs updated alongside behavior changes (`docs/RUNBOOK.md` for ops,
  `docs/DATA_DICTIONARY.md` for schema changes).
