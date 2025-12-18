from __future__ import annotations

import json
import os
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from grocery_lib.io_utils import RunPaths, ensure_dirs, read_json, atomic_write_text
from grocery_lib.notify_ardoa import notify_failure_to_ardoa

logger = logging.getLogger(__name__)


def enrich_transactions(*, scenario: str, **context) -> Dict[str, Any]:
    run_id = context["run_id"]
    base = os.getenv("ARDOA_DATA_BASE", "/opt/airflow/data")
    paths = RunPaths(base_dir=base, run_id=run_id)
    ensure_dirs(paths)

    raw_path = os.path.join(paths.raw_dir, "transactions.json")
    out_path = os.path.join(paths.out_dir, "enriched.json")

    # Ensure the output directory exists before writing
    ensure_dirs(paths.out_dir)

    payload = read_json(path=raw_path)
    txns: List[Dict[str, Any]] = payload["transactions"]

    # Schema drift handling: collect errors where expected keys are missing.
    enriched: List[Dict[str, Any]] = []
    errors: List[Dict[str, Any]] = []
    for idx, t in enumerate(txns):
        try:
            unit_price_cents = t["unit_price_cents"]
        except KeyError as exc:
            logger.error("Transaction %s missing expected key %s", idx, exc.args[0])
            errors.append({"index": idx, "missing_key": exc.args[0], "transaction": t})
            continue
        qty = int(t["quantity"])
        enriched.append(
            {
                **t,
                "revenue_cents": qty * int(unit_price_cents),
            }
        )

    result_payload = {
        "run_id": run_id,
        "rows": len(enriched),
        "enriched": enriched,
    }
    if errors:
        result_payload["errors"] = errors

    # Write enriched artifact atomically
    atomic_write_text(path=out_path, text=json.dumps(result_payload, indent=2))

    # If there were schema drift errors, surface them as a failure after artifact creation.
    if errors:
        raise KeyError("unit_price_cents missing in some transactions; see enriched artifact for details")

    return {"run_id": run_id, "enriched_path": out_path, "rows": len(enriched), "scenario": scenario}


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
    description="Enrich transactions (schema drift failure lives here)",
    tags=["grocery", "mvp", "ardoa"],
) as dag:
    scenario = "{{ dag_run.conf.get('scenario', 'ok') }}"

    t_enrich = PythonOperator(task_id="enrich_transactions", python_callable=enrich_transactions, op_kwargs={"scenario": scenario})
    t_trigger_load = TriggerDagRunOperator(
        task_id="trigger_load",
        trigger_dag_id="grocery_load_dag",
        conf={"scenario": scenario},
        wait_for_completion=False,
    )
    t_enrich >> t_trigger_load
