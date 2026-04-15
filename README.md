# Health Data Safety Platform

An interview-ready healthcare data platform that ingests messy synthetic data, cleans it, deduplicates patient identities, models the result in DuckDB, writes a compliance-friendly audit trail, and exposes monitoring in Streamlit.

## Why this project works in interviews

This repo demonstrates the full story:

- Messy source data generation in Python
- Cleaning and standardization in Python + Pandas
- Patient deduplication with explainable match rules
- Analytical modeling and marts in DuckDB SQL
- Compliance-style audit logs and data quality checks
- Monitoring dashboard in Streamlit
- Orchestration entrypoint with Prefect-compatible decorators

## Project structure

- `app/data_gen.py`: builds messy synthetic patient, encounter, claim, and lab feeds
- `app/pipeline.py`: cleans, deduplicates, logs, loads DuckDB, and writes monitoring outputs
- `streamlit_app.py`: dashboard for platform health and business-ready warehouse views
- `orchestration/flow.py`: Prefect-style orchestration entrypoint
- `sql/marts.sql`: warehouse views for patient 360, quality, and run monitoring
- `docs/interview-demo.md`: a short talk track for presenting the project

## Run locally

1. Install dependencies:

```bash
pip install -r requirements.txt
```

2. Generate raw data and build the warehouse:

```bash
python -m orchestration.flow
```

3. Open the monitoring dashboard:

```bash
streamlit run streamlit_app.py
```

If you want to force a real Prefect runtime instead of the local no-server fallback, set `ENABLE_PREFECT_RUNTIME=1` first.

## Key outputs

- DuckDB warehouse: `data/processed/healthcare_platform.duckdb`
- Audit log: `data/processed/audit_log.jsonl`
- Monitoring metrics: `data/processed/monitoring_metrics.json`
- Raw synthetic feeds: `data/raw/*.csv`

## Interview soundbite

"I built infrastructure that makes messy healthcare data safe, clean, and usable. The project simulates fragmented provider, payer, and lab feeds, applies explainable patient deduplication logic, logs every pipeline step for auditability, and exposes real-time monitoring through a Streamlit dashboard backed by DuckDB."
