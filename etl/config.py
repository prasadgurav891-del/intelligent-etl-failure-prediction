import os
from dataclasses import dataclass

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


@dataclass(frozen=True)
class Config:
    source_path: str
    batch_size: int
    progress_every: int
    db_host: str
    db_port: int
    db_name: str
    db_user: str
    db_password: str
    resource_memory_limit_bytes: int

    @classmethod
    def from_env(cls):
        return cls(
            source_path=os.getenv("GHALOGS_PATH", "Ghalogs/runs.json.gz"),
            batch_size=max(1, int(os.getenv("BATCH_SIZE", "1000"))),
            progress_every=max(1, int(os.getenv("PROGRESS_EVERY", "1000"))),
            db_host=os.getenv("DB_HOST", "localhost"),
            db_port=int(os.getenv("DB_PORT", "5432")),
            db_name=os.getenv("DB_NAME", "etl_failure_prediction"),
            db_user=os.getenv("DB_USER", "postgres"),
            db_password=os.getenv("DB_PASSWORD", ""),
            resource_memory_limit_bytes=max(1, int(os.getenv("RESOURCE_MEMORY_LIMIT_BYTES", str(32 * 1024 * 1024)))),
        )

    def db_kwargs(self):
        return {"host": self.db_host, "port": self.db_port, "dbname": self.db_name,
                "user": self.db_user, "password": self.db_password}


def effective_record_limit(requested_limit, scenario):
    """Return the one-pass source cap used by each scenario."""
    if scenario == "LARGE_DATA":
        return min(requested_limit * 5, 2**63 - 1) if requested_limit is not None else None
    return requested_limit
