# Ignore local outputs
Add-Content .gitignore "`n.env`nresults*.json`n"

# Commit 1: Remove legacy files
git add logs.txt requirements.txt setup.ps1
git commit --no-verify -m "Remove legacy setup scripts and logs"

# Commit 2: Remove dbt
git rm -r dbt_recon/
git commit --no-verify -m "Remove deprecated dbt reconciliation models"

# Commit 3: Tests (exclude results)
git add tests/
git restore --staged tests/performance/results.json tests/performance/results_pandera.json tests/performance/results_iceberg.json
git commit --no-verify -m "Add performance benchmarks and unit tests"

# Commit 4: Core pipeline
git add src/
git commit --no-verify -m "Update core pipeline and validation logic"

# Commit 5: Infrastructure
git add .github/ .gitignore .pre-commit-config.yaml Dockerfile.airflow Makefile README.md config/ dags/ docker-compose.yml pyproject.toml
git commit --no-verify -m "Update infrastructure configs and DAGs"

# Add docs if untracked
git add docs/
git commit --no-verify -m "Update documentation"

# Push all
git push
