"""
Pipeline registry — maps job names to pipeline classes.

To add a new job:
    1. Create your pipeline class in src/pipelines/
    2. Import it here and add one line to _REGISTRY.

Nothing else needs to change.
"""
from src.pipelines.generic_pipeline import GenericPipeline
# from src.pipelines.reconciliation_pipeline import ReconciliationPipeline  # example


_REGISTRY: dict[str, type] = {
    "scd":            GenericPipeline,
    # "reconciliation": ReconciliationPipeline,
}


def get_pipeline_class(job: str) -> type:
    if job not in _REGISTRY:
        raise ValueError(
            f"Unknown job: '{job}'. Available jobs: {sorted(_REGISTRY.keys())}"
        )
    return _REGISTRY[job]
