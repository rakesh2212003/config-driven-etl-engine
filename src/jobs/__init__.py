from typing import Callable
from src.jobs.raw2dis import run as raw2dis

JOB_REGISTRY: dict[str, Callable[[str], None]] = {
    "raw2dis": raw2dis,
}


def get_job(job_name: str) -> Callable[[str], None]:
    """
    Retrieve a job function from the registry by name.

    Parameters
    ----------
    job_name : name of the job (e.g. 'raw2dis')

    Returns
    -------
    Callable that accepts a table_name string
    """
    try:
        if job_name not in JOB_REGISTRY:
            available = ", ".join(JOB_REGISTRY.keys())
            raise ValueError(
                f"Unknown job '{job_name}'. Available jobs: {available}"
            )
        return JOB_REGISTRY[job_name]
    except ValueError:
        raise
    except Exception as e:
        raise RuntimeError(f"Failed to retrieve job '{job_name}': {e}") from e