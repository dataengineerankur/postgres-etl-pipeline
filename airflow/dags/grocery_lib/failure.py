from __future__ import annotations

import os
import random
from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class FailurePlan:
    """
    Deterministic failure injection for a pipeline run.
    Derived from run_id + scenario so retries are reproducible.
    """

    run_id: str
    scenario: str
    seed: int

    def rng(self) -> random.Random:
        return random.Random(self.seed)


def failure_plan(*, run_id: str, scenario: Optional[str] = None) -> FailurePlan:
    scen = scenario or os.getenv("GROCERY_SCENARIO", "ok")
    seed = abs(hash(f"{run_id}::{scen}")) % (2**32)
    return FailurePlan(run_id=run_id, scenario=scen, seed=seed)


