"""Database connection and utilities."""

import os
import sqlite3
from pathlib import Path


def get_db_path() -> Path:
    """Get the database path from environment or default location."""
    env_path = os.getenv("ROSETTA_DB_PATH")
    if env_path:
        return Path(env_path)

    # Default: look in project root
    project_root = Path(__file__).parent.parent.parent.parent
    return project_root / "clinical_rosetta.db"


def get_db_connection(db_path: Path | None = None) -> sqlite3.Connection:
    """Get a database connection with row factory."""
    if db_path is None:
        db_path = get_db_path()

    conn = sqlite3.connect(str(db_path), timeout=30)
    conn.row_factory = sqlite3.Row
    return conn


def dict_from_row(row: sqlite3.Row) -> dict:
    """Convert sqlite3.Row to dictionary."""
    return dict(zip(row.keys(), row, strict=True))
