from __future__ import annotations

import json
import os
from datetime import datetime, timedelta
from typing import Any, Dict, List

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from grocery_lib.io_utils import RunPaths, atomic_write_text, ensure_dirs, resolve_data_run_id
from grocery_lib.notify_ardoa import notify_failure_to_ardoa


def enrich_transactions(*, scenario: str, **context) -> Dict[str, Any]:
    """Enrich staged transaction records and write out/enriched.json.

    This task reads ``staged/transactions.ndjson`` and writes a JSON payload with
    a top-level ``enriched`` list.

    If the staged file is missing, raise FileNotFoundError (preserving failure
    semantics) with a clear diagnostic.
    """
    run_id = resolve_data_run_id(context)
    base = os.getenv("ARDOA_DATA_BASE", "/opt/airflow/data")
    paths = RunPaths(base_dir=base, run_id=run_id)
    ensure_dirs(paths)

    staged_path = os.path.join(paths.staged_dir, "transactions.ndjson")
    out_path = os.path.join(paths.out_dir, "enriched.json")

    if not os.path.exists(staged_path):
        raise FileNotFoundError(
            f"Staged artifact not found at {staged_path}. "
            "Upstream validation/staging may have failed or the raw input was missing."
        )

    enriched: List[Dict[str, Any]] = []
    with open(staged_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            enriched.append(json.loads(line))

    payload = {
        "run_id": run_id,
        "scenario": scenario,
        "enriched": enriched,
    }
    atomic_write_text(path=out_path, text=json.dumps(payload))

    return {"run_id": run_id, "out_path": out_path, "rows": len(enriched), "scenario": scenario}


default_args = {
    "owner": "grocery",
    "retries": 1,
    "retry_delay": timedelta(seconds=5),
    "on_failure_callback": notify_failure_to_ardoa,
}

with DAG(
    dag_id="grocery_enrich_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    description="Enrich staged transactions -> enriched.json",
    tags=["grocery", "mvp", "ardoa"],
) as dag:
    scenario = "{{ dag_run.conf.get('scenario', 'ok') }}"

    t_enrich = PythonOperator(
        task_id="enrich_transactions",
        python_callable=enrich_transactions,
        op_kwargs={"scenario": scenario},
    )

    t_trigger_load = TriggerDagRunOperator(
        task_id="trigger_load",
        trigger_dag_id="grocery_load_dag",
        conf={
            "scenario": scenario,
            "run_id": "{{ dag_run.conf.get('run_id', run_id) }}",
        },
        wait_for_completion=False,
    )

    t_enrich >> t_trigger_load
