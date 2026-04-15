from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"

DB_PATH = PROCESSED_DIR / "healthcare_platform.duckdb"
AUDIT_LOG_PATH = PROCESSED_DIR / "audit_log.jsonl"
METRICS_PATH = PROCESSED_DIR / "monitoring_metrics.json"
SQL_MARTS_PATH = ROOT_DIR / "sql" / "marts.sql"


def ensure_directories() -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
