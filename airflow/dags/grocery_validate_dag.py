from __future__ import annotations

import json
import os
from datetime import datetime, timedelta
from typing import Any, Dict, List

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from jsonschema import Draft202012Validator

from grocery_lib.io_utils import RunPaths, ensure_dirs, read_json, resolve_data_run_id
from grocery_lib.notify_ardoa import notify_failure_to_ardoa


_TXN_SCHEMA: Dict[str, Any] = {
    "type": "object",
    "properties": {
        "ok": {"type": "boolean"},
        "run_id": {"type": "string"},
        "transactions": {
            "type": "array",
            "items": {
                "type": "object",
                "required": ["event_time", "txn_id", "store_id", "sku", "quantity", "tender_type"],
                "properties": {
                    "event_time": {"type": "string"},
                    "txn_id": {"type": "string"},
                    "store_id": {"type": "string"},
                    "sku": {"type": "string"},
                    "quantity": {"type": "integer"},
                    "unit_price_cents": {"type": "integer"},
                    "tender_type": {"type": "string"},
                    "customer_id": {"type": ["string", "null"]},
                },
            },
        },
    },
    "required": ["ok", "run_id", "transactions"],
}


def validate_contract(*, scenario: str, **context) -> Dict[str, Any]:
    run_id = resolve_data_run_id(context)
    base = os.getenv("ARDOA_DATA_BASE", "/opt/airflow/data")
    paths = RunPaths(base_dir=base, run_id=run_id)
    ensure_dirs(paths)
    raw_path = os.path.join(paths.raw_dir, "transactions.json")

    # If raw file is partially written, JSONDecodeError will raise here (intentionally).
    with open(raw_path, "r", encoding="utf-8") as f:
        raw_text = f.read()
    payload = json.loads(raw_text)

    v = Draft202012Validator(_TXN_SCHEMA)
    errors = sorted(v.iter_errors(payload), key=lambda e: list(e.path))
    if errors:
        msg = "; ".join([f"{'/'.join([str(p) for p in e.path])}: {e.message}" for e in errors[:5]])
        raise RuntimeError(f"DataContractError: {msg}")

    return {"run_id": run_id, "raw_path": raw_path, "scenario": scenario, "count": len(payload.get('transactions', []))}


def stage_ndjson(**context) -> Dict[str, Any]:
    run_id = resolve_data_run_id(context)
    base = os.getenv("ARDOA_DATA_BASE", "/opt/airflow/data")
    paths = RunPaths(base_dir=base, run_id=run_id)
    raw_path = os.path.join(paths.raw_dir, "transactions.json")
    staged_path = os.path.join(paths.staged_dir, "transactions.ndjson")

    payload = read_json(path=raw_path)
    txns: List[Dict[str, Any]] = payload["transactions"]
    os.makedirs(os.path.dirname(staged_path), exist_ok=True)
    with open(staged_path, "w", encoding="utf-8") as f:
        for t in txns:
            f.write(json.dumps(t) + "\n")

    return {"run_id": run_id, "staged_path": staged_path, "rows": len(txns)}


default_args = {
    "owner": "grocery",
    "retries": 1,
    "retry_delay": timedelta(seconds=5),
    "on_failure_callback": notify_failure_to_ardoa,
}

with DAG(
    dag_id="grocery_validate_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    description="Validate contract + stage NDJSON",
    tags=["grocery", "mvp", "ardoa"],
) as dag:
    scenario = "{{ dag_run.conf.get('scenario', 'ok') }}"

    t_validate = PythonOperator(task_id="validate_contract", python_callable=validate_contract, op_kwargs={"scenario": scenario})
    t_stage = PythonOperator(task_id="stage_ndjson", python_callable=stage_ndjson)

    t_trigger_enrich = TriggerDagRunOperator(
        task_id="trigger_enrich",
        trigger_dag_id="grocery_enrich_dag",
        conf={
            "scenario": scenario,
            "run_id": "{{ dag_run.conf.get('run_id', run_id) }}",
        },
        wait_for_completion=False,
    )

    t_validate >> t_stage >> t_trigger_enrich
