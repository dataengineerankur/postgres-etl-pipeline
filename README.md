## Postgres Grocery ETL Pipeline (5 DAGs) + ARDOA Integration

This repo is a **multi-DAG** Airflow pipeline that simulates near-real grocery store transaction ingestion and loading into **Postgres**, with **deterministic failure modes** designed for ARDOA to auto-remediate via GitHub PRs.

### What you get
- **5 DAGs** that together form one end-to-end pipeline:
  - `grocery_ingest_dag`: pulls transactions from a mock POS API, writes raw artifacts (with partial write/race modes)
  - `grocery_validate_dag`: JSON schema + data contract validation
  - `grocery_enrich_dag`: enrichment + schema drift simulation
  - `grocery_load_dag`: upsert to Postgres (staging + facts)
  - `grocery_reconcile_dag`: canary checks + quarantine workflow
- Deterministic failure scenarios: malformed API payload, partial file write, schema drift, temporal failure, race condition, Postgres constraint error.
- ARDOA hooks via `on_failure_callback` that POST a `UniversalFailureEvent` to ARDOA.

### Run locally (Airflow + Postgres + mock API)
From this repo root:

```bash
cd "/Users/ankurchopra/repo_projects/postgres-etl-pipeline/airflow"
docker compose up --build -d
```

- Airflow UI: `http://localhost:8090`
- Mock POS API: `http://localhost:8199`
- Postgres: `localhost:5433` (user `airflow`, pass `airflow`, db `grocery`)

### Integrate with ARDOA (real PRs)
You run ARDOA as a separate container, mounting THIS repo as its `repo`.

1) Export your GitHub settings (token must have repo write + PR permissions):

```bash
export ARDOA_GITHUB_MODE="real"
export ARDOA_GITHUB_TOKEN="ghp_...redacted..."
export ARDOA_GITHUB_REPO="OWNER/REPO"
export GROQ_API_KEY="gsk_...redacted..."
```

2) Start ARDOA (from your ARDOA repo):

```bash
cd "/Users/ankurchopra/repo_projects/Auto-Remediation-Data-Observability-Agent-/airflow"
docker compose -p ardoa_airflow up --build -d ardoa
```

3) Point this pipeline at ARDOA:
- In `airflow/docker-compose.yml` set `ARDOA_ENDPOINT` to the ARDOA ingest URL (default already works if ARDOA is running on your host as `http://localhost:8088`).

### Trigger scenarios
Each DAG supports a `scenario` value via DAG run config. Example:

```bash
docker compose -f "/Users/ankurchopra/repo_projects/postgres-etl-pipeline/airflow/docker-compose.yml" exec -T airflow-webserver \
  airflow dags trigger grocery_ingest_dag --conf '{"scenario":"partial_write"}'
```

### Notes
- This repo is designed to be “fixed by agent”, not by editing DAG logic manually.
- ARDOA sandbox verification must pass before it opens a PR.


