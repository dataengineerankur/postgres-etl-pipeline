from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, Mapping


def resolve_data_run_id(context: Mapping[str, Any]) -> str:
    """Resolve the filesystem run id used for artifact paths.

    In triggered/chained DAGs, the producer should pass a stable identifier via
    ``dag_run.conf['run_id']`` so downstream DAGs read/write artifacts in the same
    run directory. If not present, fall back to Airflow's current ``run_id``.

    Raises KeyError if no run id can be resolved.
    """
    dag_run = context.get("dag_run")
    if dag_run is not None:
        conf = getattr(dag_run, "conf", None) or {}
        if isinstance(conf, dict):
            conf_run_id = conf.get("run_id")
            if conf_run_id:
                return str(conf_run_id)

        dr_run_id = getattr(dag_run, "run_id", None)
        if dr_run_id:
            return str(dr_run_id)

    ctx_run_id = context.get("run_id")
    if ctx_run_id:
        return str(ctx_run_id)

    raise KeyError("Unable to resolve data run id from Airflow context")


@dataclass(frozen=True)
class RunPaths:
    base_dir: str
    run_id: str

    @property
    def run_dir(self) -> str:
        return os.path.join(self.base_dir, "grocery_runs", self.run_id)

    @property
    def raw_dir(self) -> str:
        return os.path.join(self.run_dir, "raw")

    @property
    def staged_dir(self) -> str:
        return os.path.join(self.run_dir, "staged")

    @property
    def out_dir(self) -> str:
        return os.path.join(self.run_dir, "out")


def ensure_dirs(paths: RunPaths) -> None:
    os.makedirs(paths.raw_dir, exist_ok=True)
    os.makedirs(paths.staged_dir, exist_ok=True)
    os.makedirs(paths.out_dir, exist_ok=True)


def atomic_write_text(*, path: str, text: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(text)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def intentionally_non_atomic_write_text(*, path: str, text: str, pause_s: float = 1.0) -> None:
    """
    Simulates a partial file write (race-condition injector).
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        mid = max(1, len(text) // 2)
        f.write(text[:mid])
        f.flush()
        os.fsync(f.fileno())
        time.sleep(pause_s)
        f.write(text[mid:])
        f.flush()
        os.fsync(f.fileno())


def read_json(*, path: str) -> Dict[str, Any]:
    """Read a JSON file and return its contents.

    Raises a FileNotFoundError with a clear message if the file does not exist.
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"File not found: {path}. Ensure the upstream task produced this artifact.")
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)
