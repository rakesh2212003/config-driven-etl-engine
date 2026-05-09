"""File / path utility helpers."""
from pathlib import Path


def ensure_dir(path: str) -> Path:
    """Create directory (and parents) if it doesn't exist."""
    p = Path(path)
    p.mkdir(parents=True, exist_ok=True)
    return p


def list_files(directory: str, extension: str = "") -> list:
    """List all files in *directory*, optionally filtered by extension."""
    p = Path(directory)
    if not p.exists():
        return []
    pattern = f"*.{extension.lstrip('.')}" if extension else "*"
    return sorted(p.glob(pattern))
