from __future__ import annotations

import json

import streamlit as st

from app.config import DB_PATH, METRICS_PATH

try:
    import duckdb
    import pandas as pd
except ImportError as exc:  # pragma: no cover
    st.error(f"Missing dependency: {exc}. Install dependencies with `pip install -r requirements.txt`.")
    st.stop()


st.set_page_config(page_title="Health Data Safety Platform", page_icon=":hospital:", layout="wide")

st.markdown(
    """
    <style>
      :root {
        --bg-top: #d8e6ef;
        --bg-bottom: #eef3f7;
        --panel: rgba(255, 255, 255, 0.96);
        --panel-border: rgba(22, 50, 79, 0.12);
        --ink: #16324f;
        --muted: #47627d;
        --accent: #d46a2c;
      }
      .stApp {
        background:
          radial-gradient(circle at top left, rgba(132, 181, 214, 0.3), transparent 26%),
          radial-gradient(circle at top right, rgba(224, 185, 132, 0.18), transparent 20%),
          linear-gradient(180deg, var(--bg-top) 0%, var(--bg-bottom) 100%);
        color: var(--ink);
      }
      .block-container {
        padding-top: 2rem;
      }
      .hero-card {
        border: 1px solid var(--panel-border);
        border-radius: 24px;
        padding: 1.4rem 1.6rem;
        background: var(--panel);
        box-shadow: 0 18px 45px rgba(26, 55, 77, 0.1);
      }
      .eyebrow {
        color: var(--accent);
        font-size: 0.85rem;
        text-transform: uppercase;
        letter-spacing: 0.08em;
        margin-bottom: 0.3rem;
      }
      h1, h2, h3, p, label, div, span {
        color: inherit;
      }
      [data-testid="stMetric"] {
        background: var(--panel);
        border: 1px solid var(--panel-border);
        border-radius: 20px;
        padding: 1rem 1.1rem;
        box-shadow: 0 12px 26px rgba(26, 55, 77, 0.08);
      }
      [data-testid="stMetricLabel"] {
        color: var(--muted);
      }
      [data-testid="stMetricValue"] {
        color: var(--ink);
      }
      [data-baseweb="tab-list"] {
        gap: 0.5rem;
      }
      [data-baseweb="tab"] {
        background: rgba(255, 255, 255, 0.72);
        border: 1px solid var(--panel-border);
        border-radius: 999px;
        color: var(--ink);
        padding: 0.35rem 0.9rem;
      }
      [data-baseweb="tab"][aria-selected="true"] {
        background: rgba(255, 255, 255, 0.92);
        color: var(--ink);
        border: 2px solid var(--accent);
        box-shadow: 0 6px 16px rgba(212, 106, 44, 0.12);
      }
      [data-testid="stHeadingWithActionElements"] h2,
      [data-testid="stHeadingWithActionElements"] h3,
      [data-testid="stTabs"] h2,
      [data-testid="stTabs"] h3,
      [data-testid="stMarkdownContainer"],
      [data-testid="stCaptionContainer"] {
        color: var(--ink);
      }
      [data-testid="stDataFrame"],
      [data-testid="stVegaLiteChart"] {
        background: var(--panel);
        border: 1px solid var(--panel-border);
        border-radius: 20px;
        padding: 0.4rem;
        box-shadow: 0 12px 26px rgba(26, 55, 77, 0.08);
      }
      .stAlert {
        background: var(--panel);
        color: var(--ink);
        border: 1px solid var(--panel-border);
      }
    </style>
    """,
    unsafe_allow_html=True,
)

st.markdown(
    """
    <div class="hero-card">
      <h1 style="margin:0;color:#16324f;">Health Data Safety Platform</h1>
      <p style="margin:0.7rem 0 0;color:#47627d;font-size:1.05rem;">
        End-to-end infrastructure for ingesting fragmented healthcare feeds, cleaning unsafe fields,
        resolving duplicate patient identities, and monitoring platform health in real time.
      </p>
    </div>
    """,
    unsafe_allow_html=True,
)


def load_metrics() -> dict[str, object] | None:
    if not METRICS_PATH.exists():
        return None
    return json.loads(METRICS_PATH.read_text(encoding="utf-8"))


def load_query(query: str):
    if not DB_PATH.exists():
        return pd.DataFrame()
    connection = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        return connection.execute(query).df()
    finally:
        connection.close()


metrics = load_metrics()
if metrics is None:
    st.info("Run `python -m orchestration.flow` first to generate raw feeds, build the warehouse, and populate metrics.")
    st.stop()

overview = metrics["overview"]
metric_columns = st.columns(5)
metric_columns[0].metric("Raw patient rows", overview["raw_patient_rows"])
metric_columns[1].metric("Enterprise patients", overview["enterprise_patients"])
metric_columns[2].metric("Duplicates collapsed", overview["duplicate_rows_collapsed"])
metric_columns[3].metric("Claims loaded", overview["claims_loaded"])
metric_columns[4].metric("Labs loaded", overview["labs_loaded"])

tab_overview, tab_quality, tab_dedupe, tab_warehouse = st.tabs(["Overview", "Data Quality", "Deduplication", "Warehouse"])
checks_df = pd.DataFrame(metrics["checks"])

with tab_overview:
    st.subheader("Pipeline checks")
    st.dataframe(checks_df, width="stretch", hide_index=True)

    st.subheader("Run events")
    audit_df = load_query(
        """
        select event_timestamp, stage, status, message
        from mart_pipeline_monitor
        order by event_timestamp desc
        limit 25
        """
    )
    st.dataframe(audit_df, width="stretch", hide_index=True)

with tab_quality:
    st.subheader("Data quality scorecard")
    st.bar_chart(checks_df[["metric_name", "metric_value"]].set_index("metric_name"))

    st.subheader("Top duplicate clusters")
    duplicates_df = load_query(
        """
        select enterprise_patient_id, first_name, last_name, dob_clean, source_record_count, dedupe_reasons
        from dim_patients
        order by source_record_count desc, enterprise_patient_id
        limit 15
        """
    )
    st.dataframe(duplicates_df, width="stretch", hide_index=True)

with tab_dedupe:
    st.subheader("Patient identity resolution")
    merge_edges = load_query(
        """
        select reason, count(*) as merge_count
        from merge_edges
        group by reason
        order by merge_count desc
        """
    )
    if not merge_edges.empty:
        st.bar_chart(merge_edges.set_index("reason"))
    st.caption("Rules are intentionally explainable: `mrn_exact`, `email_plus_dob`, `name_dob_zip`, and `name_dob_phone4`.")

    identity_sample = load_query(
        """
        select enterprise_patient_id, source_record_id, source_system, is_survivor, dedupe_reasons, patient_fingerprint
        from patient_identity_map
        order by enterprise_patient_id, is_survivor desc
        limit 40
        """
    )
    st.dataframe(identity_sample, width="stretch", hide_index=True)

with tab_warehouse:
    st.subheader("Patient 360 mart")
    patient_360 = load_query(
        """
        select enterprise_patient_id, patient_name, city, state, source_record_count,
               total_encounters, total_claims, total_claims_paid, latest_lab_name, latest_lab_value
        from mart_patient_360
        order by total_claims_paid desc
        limit 25
        """
    )
    st.dataframe(patient_360, width="stretch", hide_index=True)

    st.subheader("Fact table volumes")
    volume_df = load_query(
        """
        select 'claims' as dataset, count(*) as row_count from fct_claims
        union all
        select 'encounters' as dataset, count(*) as row_count from fct_encounters
        union all
        select 'labs' as dataset, count(*) as row_count from fct_labs
        """
    )
    st.bar_chart(volume_df.set_index("dataset"))
