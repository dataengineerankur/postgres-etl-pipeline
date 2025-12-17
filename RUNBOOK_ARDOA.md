## Runbook: Testing ARDOA against `postgres-etl-pipeline` (real PRs + local patch testing)

### Goals
- Run the grocery pipeline locally (Airflow + Postgres + Mock POS API).
- Ensure failures trigger ARDOA.
- Ensure ARDOA:
  - generates an **agent patch**
  - **verifies** it in sandbox
  - saves the patch diff locally (so you can apply/test)
  - opens a **real GitHub PR** (not mock)

---

## 1) Start the grocery pipeline

```bash
cd "/Users/ankurchopra/repo_projects/postgres-etl-pipeline/airflow"
docker compose up --build -d
```

Airflow UI: `http://localhost:8090` (user/pass: `airflow` / `airflow`)
Mock POS API: `http://localhost:8199`

---

## 2) Start ARDOA (pointed at this repo)

ARDOA needs to read:
- the **pipeline code** (to patch)
- the **Airflow logs** + **Airflow data artifacts**

The simplest local setup is to run ARDOA with extra mounts so `file:///opt/airflow/logs/...` and `file:///opt/airflow/data/...` are readable inside the ARDOA container.

From ARDOA repo:

```bash
export ARDOA_GITHUB_MODE="real"
export ARDOA_GITHUB_REPO="OWNER/REPO"
export ARDOA_GITHUB_TOKEN="ghp_...redacted..."
export ARDOA_GITHUB_BASE_BRANCH="main"

export ARDOA_AGENT_MODE="groq"
export GROQ_API_KEY="gsk_...redacted..."

# Agent-only behavior (recommended):
export ARDOA_ALLOW_DETERMINISTIC_FALLBACK="false"
export ARDOA_VERIFY_ENABLED="true"
```

Then run ARDOA (example using `docker run` so we can mount this repo directly):

```bash
docker run --rm -p 8088:8088 \
  -e ARDOA_GITHUB_MODE -e ARDOA_GITHUB_REPO -e ARDOA_GITHUB_TOKEN -e ARDOA_GITHUB_BASE_BRANCH \
  -e ARDOA_AGENT_MODE -e GROQ_API_KEY -e ARDOA_ALLOW_DETERMINISTIC_FALLBACK -e ARDOA_VERIFY_ENABLED \
  -e ARDOA_PUBLIC_BASE_URL="http://localhost:8088" \
  -e ARDOA_REPO_ROOT="repo" \
  -v "/Users/ankurchopra/repo_projects/postgres-etl-pipeline:/opt/ardoa/repo:rw" \
  -v "/Users/ankurchopra/repo_projects/postgres-etl-pipeline/airflow/logs:/opt/airflow/logs:ro" \
  -v "/Users/ankurchopra/repo_projects/postgres-etl-pipeline/airflow/data:/opt/airflow/data:ro" \
  ardoa:latest
```

Event console: `http://localhost:8088/ui`

---

## 3) Trigger failure scenarios

Trigger ingest with a scenario:

```bash
docker compose -f "/Users/ankurchopra/repo_projects/postgres-etl-pipeline/airflow/docker-compose.yml" exec -T airflow-webserver \
  airflow dags trigger grocery_ingest_dag --conf '{"scenario":"partial_write"}'
```

Useful scenarios:
- `partial_write`: downstream JSON decode failure
- `schema_drift`: enrich step KeyError on missing `unit_price_cents`
- `temporal_error`: mock API returns 500 (upstream availability)
- `malformed_json`: mock API returns invalid JSON body

---

## 4) Test the agent’s fix locally BEFORE reviewing PR

When ARDOA produces a verified patch, it logs `patch.saved` and the UI shows a **download diff** link:
- `http://localhost:8088/api/patch/<patch_id>.diff`

Apply it to your local checkout:

```bash
cd "/Users/ankurchopra/repo_projects/postgres-etl-pipeline"
git checkout -b "ardoa/local-test"
curl -s "http://localhost:8088/api/patch/<patch_id>.diff" > /tmp/ardoa.patch.diff
git apply /tmp/ardoa.patch.diff
```

Re-run the scenario after applying the patch:

```bash
cd "/Users/ankurchopra/repo_projects/postgres-etl-pipeline/airflow"
docker compose restart airflow-scheduler airflow-webserver
docker compose exec -T airflow-webserver airflow dags trigger grocery_ingest_dag --conf '{"scenario":"partial_write"}'
```

---

## 5) Confirm real PR creation

Once ARDOA verification passes, ARDOA will open a real PR in `ARDOA_GITHUB_REPO` on a branch like:
- `ardoa/<patch_id_prefix>`

You’ll see:
- `pr.created` in `http://localhost:8088/ui`


