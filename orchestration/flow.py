from __future__ import annotations

import json
import os

from app.data_gen import generate_raw_data
from app.pipeline import run_pipeline

try:
    if os.getenv("ENABLE_PREFECT_RUNTIME") == "1":
        from prefect import flow, task
    else:
        raise ImportError
except ImportError:  # pragma: no cover
    def _identity_decorator(func=None, **_kwargs):
        if func is None:
            return lambda inner: inner
        return func

    flow = _identity_decorator
    task = _identity_decorator


@task(name="generate synthetic feeds")
def generate_task() -> dict[str, int]:
    return generate_raw_data()


@task(name="build healthcare warehouse")
def pipeline_task() -> dict[str, object]:
    return run_pipeline()


@flow(name="healthcare-data-safety-platform")
def healthcare_data_safety_flow() -> dict[str, object]:
    raw_counts = generate_task()
    pipeline_summary = pipeline_task()
    return {
        "raw_counts": raw_counts,
        "pipeline_summary": pipeline_summary,
    }


if __name__ == "__main__":
    print(json.dumps(healthcare_data_safety_flow(), indent=2))
