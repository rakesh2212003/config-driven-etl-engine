from typing import Callable
from src.jobs.raw2dis import run as raw2dis

JOB_REGISTRY: dict[str, Callable[[str], None]] = {
    "raw2dis": raw2dis,
}

def get_job(job_name:str) -> callable[[str], None]:
    if job_name not in JOB_REGISTRY:
        available = ", ".join(JOB_REGISTRY.keys())
        raise ValueError(f"unknown job '{job_name}. Available jobs: {available}")
    return JOB_REGISTRY[job_name]