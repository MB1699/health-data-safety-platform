from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime

from app.config import AUDIT_LOG_PATH, DB_PATH, METRICS_PATH, RAW_DIR, SQL_MARTS_PATH, ensure_directories

try:
    import duckdb
    import pandas as pd
except ImportError as exc:  # pragma: no cover
    duckdb = None
    pd = None
    IMPORT_ERROR = exc
else:
    IMPORT_ERROR = None


def require_stack() -> None:
    if duckdb is None or pd is None:
        raise RuntimeError(
            "Missing required packages for the healthcare pipeline. Install dependencies with "
            "`pip install -r requirements.txt`."
        ) from IMPORT_ERROR


def _safe_text(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _normalize_name(value: object) -> str:
    return "".join(character for character in _safe_text(value).lower() if character.isalpha())


def _normalize_phone(value: object) -> str:
    digits = "".join(character for character in _safe_text(value) if character.isdigit())
    return digits[-10:]


def _normalize_zip(value: object) -> str:
    digits = "".join(character for character in _safe_text(value) if character.isdigit())
    return digits[:5]


def _normalize_email(value: object) -> str:
    return _safe_text(value).lower()


def _normalize_gender(value: object) -> str:
    lowered = _safe_text(value).lower()
    if lowered in {"f", "female", "woman"}:
        return "female"
    if lowered in {"m", "male", "man"}:
        return "male"
    return "unknown"


def _record_fingerprint(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:12]


def _parse_datetime_value(value: object, formats: list[str] | None = None):
    require_stack()
    text = _safe_text(value)
    if not text:
        return pd.NaT
    for date_format in formats or []:
        try:
            return pd.Timestamp(datetime.strptime(text, date_format))
        except ValueError:
            continue
    return pd.to_datetime(text, errors="coerce")


def _load_csv(filename: str):
    require_stack()
    return pd.read_csv(RAW_DIR / filename)


def _clean_patients(df):
    require_stack()
    clean = df.copy()
    clean["first_name"] = clean["first_name"].fillna("").astype(str).str.strip().str.title()
    clean["last_name"] = clean["last_name"].fillna("").astype(str).str.strip().str.title()
    clean["email"] = clean["email"].fillna("").astype(str).str.strip().str.lower()
    clean["phone"] = clean["phone"].fillna("").astype(str)
    clean["address"] = clean["address"].fillna("").astype(str).str.strip().str.title()
    clean["city"] = clean["city"].fillna("").astype(str).str.strip().str.title()
    clean["state"] = clean["state"].fillna("").astype(str).str.strip().str.upper()
    clean["zip_code"] = clean["zip_code"].fillna("").astype(str)
    clean["mrn"] = clean["mrn"].fillna("").astype(str).str.strip().str.upper()
    clean["updated_at"] = pd.to_datetime(clean["updated_at"], errors="coerce")
    clean["dob_clean"] = clean["dob"].map(
        lambda value: _parse_datetime_value(value, ["%Y-%m-%d", "%m/%d/%Y", "%d-%b-%Y", "%Y/%m/%d"])
    )
    clean["first_name_norm"] = clean["first_name"].map(_normalize_name)
    clean["last_name_norm"] = clean["last_name"].map(_normalize_name)
    clean["phone_norm"] = clean["phone"].map(_normalize_phone)
    clean["phone_last4"] = clean["phone_norm"].str[-4:]
    clean["zip_norm"] = clean["zip_code"].map(_normalize_zip)
    clean["email_norm"] = clean["email"].map(_normalize_email)
    clean["gender_clean"] = clean["gender"].map(_normalize_gender)
    clean["patient_name_key"] = (
        clean["first_name_norm"] + "|" + clean["last_name_norm"] + "|" + clean["dob_clean"].dt.strftime("%Y-%m-%d").fillna("")
    )
    clean["completeness_score"] = (
        clean["mrn"].ne("").astype(int)
        + clean["email_norm"].ne("").astype(int)
        + clean["phone_norm"].ne("").astype(int)
        + clean["address"].ne("").astype(int)
        + clean["zip_norm"].ne("").astype(int)
    )
    return clean


def _clean_events(df, date_columns):
    require_stack()
    clean = df.copy()
    for column in date_columns:
        clean[column] = pd.to_datetime(clean[column], errors="coerce")
    return clean


@dataclass
class UnionFind:
    parent: dict[int, int]
    rank: dict[int, int]

    @classmethod
    def from_items(cls, items: list[int]) -> "UnionFind":
        return cls(parent={item: item for item in items}, rank={item: 0 for item in items})

    def find(self, item: int) -> int:
        if self.parent[item] != item:
            self.parent[item] = self.find(self.parent[item])
        return self.parent[item]

    def union(self, left: int, right: int) -> bool:
        left_root = self.find(left)
        right_root = self.find(right)
        if left_root == right_root:
            return False
        if self.rank[left_root] < self.rank[right_root]:
            left_root, right_root = right_root, left_root
        self.parent[right_root] = left_root
        if self.rank[left_root] == self.rank[right_root]:
            self.rank[left_root] += 1
        return True


def _apply_match_rule(df, union_find: UnionFind, edge_log: list[dict[str, str]], columns: list[str], reason: str) -> None:
    valid = df.copy()
    for column in columns:
        valid = valid[valid[column].notna() & valid[column].astype(str).ne("")]

    if valid.empty:
        return

    for _, index_values in valid.groupby(columns).groups.items():
        matches = list(index_values)
        if len(matches) < 2:
            continue
        anchor = matches[0]
        for candidate in matches[1:]:
            merged = union_find.union(anchor, candidate)
            if merged:
                edge_log.append(
                    {
                        "left_record": df.at[anchor, "source_record_id"],
                        "right_record": df.at[candidate, "source_record_id"],
                        "reason": reason,
                    }
                )


def _deduplicate_patients(df):
    require_stack()
    working = df.copy()
    union_find = UnionFind.from_items(list(working.index))
    edge_log: list[dict[str, str]] = []

    _apply_match_rule(working, union_find, edge_log, ["mrn"], "mrn_exact")
    _apply_match_rule(working, union_find, edge_log, ["email_norm", "dob_clean"], "email_plus_dob")
    _apply_match_rule(working, union_find, edge_log, ["first_name_norm", "last_name_norm", "dob_clean", "zip_norm"], "name_dob_zip")
    _apply_match_rule(working, union_find, edge_log, ["first_name_norm", "last_name_norm", "dob_clean", "phone_last4"], "name_dob_phone4")

    working["cluster_root"] = [union_find.find(index) for index in working.index]
    cluster_roots = {root: position + 1 for position, root in enumerate(sorted(working["cluster_root"].unique()))}
    working["enterprise_patient_id"] = working["cluster_root"].map(lambda root: f"EPI-{cluster_roots[root]:05d}")
    working["source_record_count"] = working.groupby("enterprise_patient_id")["source_record_id"].transform("count")

    reason_map: dict[str, set[str]] = {record_id: set() for record_id in working["source_record_id"]}
    for edge in edge_log:
        reason_map[edge["left_record"]].add(edge["reason"])
        reason_map[edge["right_record"]].add(edge["reason"])
    working["dedupe_reasons"] = working["source_record_id"].map(
        lambda record_id: ", ".join(sorted(reason_map[record_id])) if reason_map[record_id] else "unique_record"
    )
    working["patient_fingerprint"] = working["source_record_id"].map(_record_fingerprint)

    working = working.sort_values(["enterprise_patient_id", "completeness_score", "updated_at"], ascending=[True, False, False])
    working["is_survivor"] = ~working.duplicated(subset=["enterprise_patient_id"])

    survivors = working[working["is_survivor"]].copy()
    survivors["duplicate_rows_collapsed"] = survivors["source_record_count"] - 1

    return working, survivors, edge_log


def _build_metric_records(run_id: str, patients_raw, identity_map, claims, labs, encounters):
    require_stack()
    invalid_dob_count = int(patients_raw["dob_clean"].isna().sum())
    duplicate_rows_collapsed = int(len(identity_map) - identity_map["enterprise_patient_id"].nunique())
    orphan_claims = int(claims["enterprise_patient_id"].isna().sum())
    orphan_labs = int(labs["enterprise_patient_id"].isna().sum())
    orphan_encounters = int(encounters["enterprise_patient_id"].isna().sum())

    return [
        {
            "run_id": run_id,
            "metric_name": "invalid_dob_rate",
            "metric_value": round(invalid_dob_count / max(len(patients_raw), 1), 4),
            "threshold": 0.08,
            "status": "pass" if invalid_dob_count / max(len(patients_raw), 1) <= 0.08 else "fail",
        },
        {
            "run_id": run_id,
            "metric_name": "duplicate_rows_collapsed",
            "metric_value": duplicate_rows_collapsed,
            "threshold": 1,
            "status": "pass" if duplicate_rows_collapsed > 0 else "warn",
        },
        {
            "run_id": run_id,
            "metric_name": "orphan_claim_rows",
            "metric_value": orphan_claims,
            "threshold": 0,
            "status": "pass" if orphan_claims == 0 else "fail",
        },
        {
            "run_id": run_id,
            "metric_name": "orphan_lab_rows",
            "metric_value": orphan_labs,
            "threshold": 0,
            "status": "pass" if orphan_labs == 0 else "fail",
        },
        {
            "run_id": run_id,
            "metric_name": "orphan_encounter_rows",
            "metric_value": orphan_encounters,
            "threshold": 0,
            "status": "pass" if orphan_encounters == 0 else "fail",
        },
    ]


def _write_audit_log(audit_records: list[dict[str, str]]) -> None:
    AUDIT_LOG_PATH.write_text("\n".join(json.dumps(record, default=str) for record in audit_records), encoding="utf-8")


def _load_sql_views(connection) -> None:
    connection.execute(SQL_MARTS_PATH.read_text(encoding="utf-8"))


def run_pipeline() -> dict[str, object]:
    require_stack()
    ensure_directories()

    run_id = datetime.utcnow().strftime("run_%Y%m%dT%H%M%SZ")
    audit_records: list[dict[str, object]] = []

    def log(stage: str, status: str, message: str, payload: dict[str, object] | None = None) -> None:
        audit_records.append(
            {
                "event_timestamp": datetime.utcnow().isoformat(timespec="seconds"),
                "run_id": run_id,
                "stage": stage,
                "status": status,
                "message": message,
                "payload": payload or {},
            }
        )

    log("pipeline", "started", "Healthcare platform pipeline started.")

    patients_raw = _clean_patients(_load_csv("patients_raw.csv"))
    encounters = _clean_events(_load_csv("encounters_raw.csv"), ["admit_at", "discharge_at"])
    claims = _clean_events(_load_csv("claims_raw.csv"), ["claim_date"])
    labs = _clean_events(_load_csv("labs_raw.csv"), ["collected_at"])

    log(
        "ingest",
        "completed",
        "Raw feeds loaded from disk.",
        {
            "patients_raw": len(patients_raw),
            "encounters_raw": len(encounters),
            "claims_raw": len(claims),
            "labs_raw": len(labs),
        },
    )

    identity_map, dim_patients, edge_log = _deduplicate_patients(patients_raw)
    patient_keys = identity_map[["source_record_id", "enterprise_patient_id"]]
    claims = claims.merge(patient_keys, left_on="source_patient_id", right_on="source_record_id", how="left").drop(columns=["source_record_id"])
    labs = labs.merge(patient_keys, left_on="source_patient_id", right_on="source_record_id", how="left").drop(columns=["source_record_id"])
    encounters = encounters.merge(patient_keys, left_on="source_patient_id", right_on="source_record_id", how="left").drop(columns=["source_record_id"])

    log(
        "dedupe",
        "completed",
        "Patient identity graph resolved duplicates into enterprise records.",
        {
            "enterprise_patients": int(dim_patients["enterprise_patient_id"].nunique()),
            "duplicate_rows_collapsed": int(len(identity_map) - len(dim_patients)),
            "merge_edges": len(edge_log),
        },
    )

    metric_records = _build_metric_records(run_id, patients_raw, identity_map, claims, labs, encounters)
    connection = duckdb.connect(str(DB_PATH))
    try:
        connection.register("patients_raw_df", patients_raw)
        connection.register("identity_map_df", identity_map)
        connection.register("dim_patients_df", dim_patients)
        connection.register("claims_df", claims)
        connection.register("labs_df", labs)
        connection.register("encounters_df", encounters)
        connection.register("monitoring_metrics_df", pd.DataFrame(metric_records))
        connection.register("audit_records_df", pd.DataFrame(audit_records))
        connection.register("merge_edges_df", pd.DataFrame(edge_log))

        connection.execute("create or replace table raw_patients as select * from patients_raw_df")
        connection.execute("create or replace table patient_identity_map as select * from identity_map_df")
        connection.execute("create or replace table dim_patients as select * from dim_patients_df")
        connection.execute("create or replace table fct_claims as select * from claims_df")
        connection.execute("create or replace table fct_labs as select * from labs_df")
        connection.execute("create or replace table fct_encounters as select * from encounters_df")
        connection.execute("create or replace table monitoring_metrics as select * from monitoring_metrics_df")
        connection.execute("create or replace table merge_edges as select * from merge_edges_df")
        connection.execute("create or replace table pipeline_run_audit as select * from audit_records_df")
        _load_sql_views(connection)
    finally:
        connection.close()

    summary = {
        "run_id": run_id,
        "generated_at": datetime.utcnow().isoformat(timespec="seconds"),
        "overview": {
            "raw_patient_rows": int(len(patients_raw)),
            "enterprise_patients": int(dim_patients["enterprise_patient_id"].nunique()),
            "duplicate_rows_collapsed": int(len(identity_map) - len(dim_patients)),
            "claims_loaded": int(len(claims)),
            "labs_loaded": int(len(labs)),
            "encounters_loaded": int(len(encounters)),
        },
        "checks": metric_records,
        "top_duplicate_clusters": (
            dim_patients[["enterprise_patient_id", "source_record_count", "dedupe_reasons"]]
            .sort_values("source_record_count", ascending=False)
            .head(10)
            .to_dict(orient="records")
        ),
    }

    log("warehouse", "completed", "DuckDB marts refreshed and ready for dashboard queries.")
    log("pipeline", "completed", "Healthcare platform pipeline completed successfully.", summary["overview"])
    connection = duckdb.connect(str(DB_PATH))
    try:
        connection.register("audit_records_df", pd.DataFrame(audit_records))
        connection.execute("create or replace table pipeline_run_audit as select * from audit_records_df")
        _load_sql_views(connection)
    finally:
        connection.close()

    _write_audit_log(audit_records)
    METRICS_PATH.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    return summary


if __name__ == "__main__":
    print(json.dumps(run_pipeline(), indent=2))
