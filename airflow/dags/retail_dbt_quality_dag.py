from __future__ import annotations

import json
import os
import subprocess
from datetime import datetime, timedelta
from typing import Any, Dict

from airflow import DAG
from airflow.operators.python import PythonOperator


def _write_profiles(**context: Any) -> Dict[str, Any]:
    dr = context.get("dag_run")
    scenario = (getattr(dr, "conf", {}) or {}).get("scenario", "ok")

    profiles_dir = os.getenv("DBT_PROFILES_DIR", "/opt/airflow/data/dbt_profiles")
    os.makedirs(profiles_dir, exist_ok=True)

    profiles_yml = f"""
dbt_retail:
  target: dev
  outputs:
    dev:
      type: postgres
      host: postgres
      user: airflow
      password: airflow
      port: 5432
      dbname: airflow
      schema: retail_stg
      threads: 2
"""
    path = os.path.join(profiles_dir, "profiles.yml")
    with open(path, "w", encoding="utf-8") as f:
        f.write(profiles_yml.strip() + "\n")

    return {"scenario": scenario, "profiles_dir": profiles_dir}


def _dbt_test(**context: Any) -> Dict[str, Any]:
    x = context["ti"].xcom_pull(task_ids="write_profiles") or {}
    scenario = x.get("scenario", "ok")
    profiles_dir = x.get("profiles_dir", "/opt/airflow/data/dbt_profiles")

    proj = "/opt/airflow/dbt_retail"
    env = dict(os.environ)
    env["DBT_PROFILES_DIR"] = profiles_dir
    vars_json = json.dumps({"scenario": scenario})

    # Run tests (includes assert_non_empty_mart.sql which fails for empty marts)
    cmd = ["dbt", "test", "--project-dir", proj, "--vars", vars_json]
    proc = subprocess.run(cmd, check=False, env=env, cwd=proj, text=True, capture_output=True)
    if proc.returncode != 0:
        out = (proc.stdout or "") + "\n" + (proc.stderr or "")
        tail = "\n".join(out.splitlines()[-220:])
        raise RuntimeError(f"dbt test failed (rc={proc.returncode}). tail:\n{tail}")
    return x


with DAG(
    dag_id="retail_dbt_quality_dag",
    description="Run dbt tests / contracts for retail pipeline (no ARDOA hints).",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 0, "retry_delay": timedelta(seconds=5)},
) as dag:
    t_profiles = PythonOperator(task_id="write_profiles", python_callable=_write_profiles)
    t_test = PythonOperator(task_id="dbt_test", python_callable=_dbt_test)

    t_profiles >> t_test


