from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple

from sqlalchemy import create_engine, text


@dataclass(frozen=True)
class PgConfig:
    dsn: str = os.getenv("GROCERY_PG_DSN", "postgresql+psycopg2://airflow:airflow@postgres:5432/grocery")


def _engine(cfg: Optional[PgConfig] = None):
    c = cfg or PgConfig()
    return create_engine(c.dsn, pool_pre_ping=True)


def exec_sql(sql: str, params: Optional[Dict[str, Any]] = None, *, cfg: Optional[PgConfig] = None) -> None:
    eng = _engine(cfg)
    with eng.begin() as conn:
        conn.execute(text(sql), params or {})


def fetch_all(sql: str, params: Optional[Dict[str, Any]] = None, *, cfg: Optional[PgConfig] = None) -> List[Tuple[Any, ...]]:
    eng = _engine(cfg)
    with eng.begin() as conn:
        res = conn.execute(text(sql), params or {})
        return list(res.fetchall())


def upsert_stg_transactions(rows: Iterable[Dict[str, Any]], *, cfg: Optional[PgConfig] = None) -> int:
    """
    Minimal upsert into grocery.stg_transactions keyed by (run_id, txn_id).
    """
    eng = _engine(cfg)
    sql = text(
        """
        INSERT INTO grocery.stg_transactions
          (run_id, event_time, txn_id, store_id, sku, quantity, unit_price_cents, tender_type, customer_id, raw_payload)
        VALUES
          (:run_id, :event_time, :txn_id, :store_id, :sku, :quantity, :unit_price_cents, :tender_type, :customer_id, :raw_payload::jsonb)
        ON CONFLICT (run_id, txn_id) DO UPDATE SET
          event_time = EXCLUDED.event_time,
          store_id = EXCLUDED.store_id,
          sku = EXCLUDED.sku,
          quantity = EXCLUDED.quantity,
          unit_price_cents = EXCLUDED.unit_price_cents,
          tender_type = EXCLUDED.tender_type,
          customer_id = EXCLUDED.customer_id,
          raw_payload = EXCLUDED.raw_payload
        """
    )
    count = 0
    with eng.begin() as conn:
        for r in rows:
            conn.execute(sql, r)
            count += 1
    return count


