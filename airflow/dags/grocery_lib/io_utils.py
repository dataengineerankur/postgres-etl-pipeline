from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from typing import Any, Dict


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
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


