# Zenodo upload

## What to upload (payload)

Include — all original, CC BY 4.0 (`LICENSE`):

- `sql/questions/` (11 dashboard queries)
- `docs/fig/fig1_match_mix.svg` … `fig4_top_merchants_settlement.svg`
  (rebuild with `make figures`)
- `data/metrics_20260904.json` (aggregate counts only: 8813 / 500 / 492 on 9805)
- `scripts/make_figures.py`, `scripts/refresh_dashboard.py`
- `src/`, `tests/fixtures/recon_cases.yaml`, `config/fee_rates.yaml`
- `docs/` (`DASHBOARD.md`, `ARCHITECTURE.md`, `BENCHMARKS.md`, `REAL_DATA.md`,
  `PROOF.md`, `DATA_DICTIONARY.md`, `RUNBOOK.md`), `README.md`, `LICENSE`,
  `.zenodo.json`

Exclude — third-party-derived, unlicensed upstream
(`thiru1711/Financial_Transactions`, no license card per `SECURITY.md`):

- `data/real_thiru_full_*`, `data/samples/real_10k_*`,
  `data/settlement_*.csv`, `data/curated_*.csv`, quarantine CSVs, DLQ dumps

## Steps (zenodo.org → New upload)

1. `make figures` — confirm `docs/fig/` regenerates clean.
2. Zip the include-list above (no `data/samples/`, no full CSVs).
3. New upload → fill title/creators from `.zenodo.json` → license
   **CC-BY-4.0** → keywords: reconciliation, apache-iceberg, trino.
4. Description: one-leg-real pattern (gateway real, settlement derived),
   89.88% mix on 9805-row 10k sample; 89.98% on 12.6M local run (unarchived).
5. Publish → copy the DOI back into `README.md` + this file.

## Why the rows stay out

Provenance risk accepted for local use is recorded in `SECURITY.md`;
redistribution is a different act. Aggregates (counts, sums, rates) are
facts we computed — safe to publish. Row-level gateway amounts/dates/
merchants trace to the unlicensed upstream — keep them local.
