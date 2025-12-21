from __future__ import annotations

import os
from datetime import datetime, timedelta
from typing import Any, Dict

import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


def _pg_dsn() -> str:
    # Docker network DSN (works inside Airflow containers)
    return os.getenv("RETAIL_PG_DSN", "postgresql://airflow:airflow@postgres:5432/airflow")


def _exec_sql(sql: str) -> None:
    with psycopg2.connect(_pg_dsn()) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(sql)


def init_seed(**context: Any) -> Dict[str, Any]:
    dr = context.get("dag_run")
    scenario = (getattr(dr, "conf", {}) or {}).get("scenario", "ok")
    run_id = getattr(dr, "run_id", "") or ""
    return {"scenario": scenario, "run_id": run_id}


def create_schema_and_table(**context: Any) -> Dict[str, Any]:
    x = context["ti"].xcom_pull(task_ids="init_seed") or {}
    scenario = x.get("scenario", "ok")

    # Note: amount_cents stored as TEXT to simulate bad-data issues without blocking inserts.
    # For most scenarios (including model_bug), we want seeding to succeed even if a prior dbt run
    # left behind dependent views. For strict dependency testing, use scenario=dependency_issue.
    drop_stmt = (
        "drop table if exists retail_src.transactions_src;"
        if scenario == "dependency_issue"
        else "drop table if exists retail_src.transactions_src cascade;"
    )
    _exec_sql(
        f"""
        create schema if not exists retail_src;
        create schema if not exists retail_analytics;
        {drop_stmt}
        create table retail_src.transactions_src (
          transaction_id text not null,
          store_id integer not null,
          sku text not null,
          amount_cents text not null,
          quantity integer not null,
          transaction_ts timestamp not null
        );
        """
    )

    if scenario == "schema_drift":
        # Drift: rename quantity -> qty, but dbt model still expects quantity
        _exec_sql(
            """
            alter table retail_src.transactions_src drop column quantity;
            alter table retail_src.transactions_src add column qty integer not null default 1;
            """
        )

    return x


def insert_source_rows(**context: Any) -> Dict[str, Any]:
    x = context["ti"].xcom_pull(task_ids="init_seed") or {}
    scenario = x.get("scenario", "ok")

    if scenario == "upstream_missing":
        # Upstream issue: table exists, but no rows produced
        return x

    if scenario == "race_partial":
        # Intentionally skip inserts in this task; inserts happen later (after dbt starts) to simulate a race.
        return x

    # Normal + other scenarios insert rows
    if scenario == "bad_data":
        # Insert a non-numeric amount to create a cast error downstream in dbt
        _exec_sql(
            """
            insert into retail_src.transactions_src(transaction_id, store_id, sku, amount_cents, quantity, transaction_ts)
            values
              ('t1', 101, 'banana', '199', 1, now()),
              ('t2', 101, 'apple', 'oops', 2, now());
            """
        )
    elif scenario == "schema_drift":
        _exec_sql(
            """
            insert into retail_src.transactions_src(transaction_id, store_id, sku, amount_cents, qty, transaction_ts)
            values
              ('t1', 101, 'banana', '199', 1, now()),
              ('t2', 102, 'milk', '499', 1, now());
            """
        )
    else:
        _exec_sql(
            """
            insert into retail_src.transactions_src(transaction_id, store_id, sku, amount_cents, quantity, transaction_ts)
            values
              ('t1', 101, 'banana', '199', 1, now()),
              ('t2', 101, 'apple', '299', 2, now()),
              ('t3', 102, 'milk', '499', 1, now());
            """
        )
    return x


def delayed_insert_for_race(**context: Any) -> Dict[str, Any]:
    x = context["ti"].xcom_pull(task_ids="init_seed") or {}
    scenario = x.get("scenario", "ok")
    if scenario != "race_partial":
        return x

    # Inserts happen later to simulate a producer/consumer race (dbt sees empty table first).
    _exec_sql(
        """
        insert into retail_src.transactions_src(transaction_id, store_id, sku, amount_cents, quantity, transaction_ts)
        values
          ('t_race1', 201, 'bread', '599', 1, now()),
          ('t_race2', 201, 'butter', '799', 1, now());
        """
    )
    return x


with DAG(
    dag_id="retail_seed_dag",
    description="Seed retail source table in Postgres with scenario-driven failure injection (no ARDOA hints).",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(seconds=5)},
) as dag:
    t_init = PythonOperator(task_id="init_seed", python_callable=init_seed)
    t_create = PythonOperator(task_id="create_schema_and_table", python_callable=create_schema_and_table)
    t_insert = PythonOperator(task_id="insert_source_rows", python_callable=insert_source_rows)
    t_delayed = PythonOperator(task_id="delayed_insert_for_race", python_callable=delayed_insert_for_race)

    trigger_dbt = TriggerDagRunOperator(
        task_id="trigger_retail_dbt_run",
        trigger_dag_id="retail_dbt_run_dag",
        conf={"scenario": "{{ dag_run.conf.get('scenario', 'ok') }}"},
        # For test harness UX we want the "seed" DAG run to reflect end-to-end success/failure.
        # If dbt fails, this task (and DAG) should fail too, so a single trigger command is enough.
        wait_for_completion=True,
        allowed_states=["success"],
        failed_states=["failed"],
        poke_interval=10,
    )

    t_init >> t_create >> t_insert >> trigger_dbt
    # For race scenario, delayed insert happens *after* triggering dbt to create the race.
    trigger_dbt >> t_delayed


