from __future__ import annotations

import json
import random
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from faker import Faker
from fastapi import FastAPI, HTTPException, Query

app = FastAPI(title="Mock POS API", version="0.1")
fake = Faker()


def _rng(run_id: str, scenario: str) -> random.Random:
    seed = abs(hash(f"{run_id}::{scenario}")) % (2**32)
    return random.Random(seed)


def _txn(rng: random.Random, *, store_id: str) -> Dict[str, Any]:
    sku = rng.choice(["SKU-APPLE", "SKU-MILK", "SKU-BREAD", "SKU-COFFEE", "SKU-RICE"])
    qty = rng.randint(1, 5)
    unit_price_cents = rng.choice([199, 299, 399, 599, 899])
    tender = rng.choice(["cash", "card", "ebt"])
    event_time = datetime.now(timezone.utc).isoformat()
    return {
        "ok": True,
        "event_time": event_time,
        "txn_id": fake.uuid4(),
        "store_id": store_id,
        "sku": sku,
        "quantity": qty,
        "unit_price_cents": unit_price_cents,
        "tender_type": tender,
        "customer_id": fake.uuid4() if rng.random() < 0.6 else None,
    }


@app.get("/transactions")
def transactions(
    *,
    run_id: str = Query(...),
    scenario: str = Query("ok"),
    n: int = Query(25, ge=1, le=200),
    store_id: Optional[str] = Query(None),
) -> Any:
    """
    Deterministic failure injection by `scenario`.

    Scenarios:
    - ok: normal response
    - temporal_error: intermittently returns 500 with JSON error payload
    - malformed_json: returns a truncated JSON string (invalid JSON)
    - schema_drift: removes/renames a field
    """
    rng = _rng(run_id, scenario)
    sid = store_id or rng.choice(["SFO-001", "NYC-014", "AUS-002"])

    if scenario == "temporal_error":
        # Deterministic “flaky” pattern based on seed:
        if rng.random() < 0.7:
            raise HTTPException(
                status_code=500,
                detail={"error": "upstream_unavailable", "run_id": run_id, "retry_after_s": 2},
            )

    txns: List[Dict[str, Any]] = [_txn(rng, store_id=sid) for _ in range(n)]

    if scenario == "schema_drift":
        # Rename `unit_price_cents` -> `unit_price` for the first record (drift)
        t0 = dict(txns[0])
        t0["unit_price"] = t0.pop("unit_price_cents")
        txns[0] = t0

    if scenario == "malformed_json":
        # Return an invalid JSON payload (partial/truncated)
        good = {"ok": True, "run_id": run_id, "transactions": txns}
        raw = json.dumps(good)
        return raw[: max(1, len(raw) // 2)]

    return {"ok": True, "run_id": run_id, "transactions": txns}


