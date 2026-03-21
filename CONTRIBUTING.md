# Contributing to Fleet Reliability Pipeline

Thank you for your interest in contributing! This document explains how to get started.

## Development Setup
```bash
# 1. Fork and clone the repo
git clone https://github.com/YOUR_USERNAME/fleet-reliability-pipeline.git
cd fleet-reliability-pipeline

# 2. Create conda environment
conda create -n fleet-env python=3.12 -y
conda activate fleet-env
pip install -r requirements.txt

# 3. Start the database
docker-compose up -d

# 4. Generate data and run pipeline
python data/generate_mock_data.py
python etl/ingest.py
python etl/clean.py
python etl/transform.py

# 5. Run tests before making changes
pytest tests/ -v
```

## Branching Strategy

| Branch | Purpose |
|---|---|
| `main` | Production — always deployable |
| `feature/xxx` | New features |
| `fix/xxx` | Bug fixes |
| `docs/xxx` | Documentation only |

Always branch off `main`:
```bash
git checkout main
git pull origin main
git checkout -b feature/your-feature-name
```

## Pull Request Process

1. Create a branch from `main`
2. Make your changes
3. Run the full test suite — all tests must pass
4. Run dbt tests — all 35 must pass
5. Open a PR with a clear description of what changed and why
6. PR title must follow: `feat:`, `fix:`, `docs:`, `refactor:`, `test:`

## Code Standards

- Python: formatted with `ruff` (line length 100)
- SQL: dbt models only — no raw SQL in Python files for transforms
- All new ETL logic must have a corresponding pytest test
- All new dbt models must have a schema.yml entry with at least `not_null` and `unique` tests

## Project Structure
```
fleet-reliability-pipeline/
├── data/          # Data generation scripts
├── etl/           # Ingest, clean, transform
├── dags/          # Airflow DAG
├── dbt/           # dbt models (staging → intermediate → marts)
├── models/        # ML forecasting (Prophet)
├── dashboard/     # Streamlit app
├── tests/         # pytest unit tests
└── scripts/       # One-time utility scripts
```

## Reporting Issues

Open a GitHub Issue with:
- What you expected to happen
- What actually happened
- Steps to reproduce
- Your Python version and OS

## Questions?

Open a GitHub Discussion or reach out via the Issues tab.
