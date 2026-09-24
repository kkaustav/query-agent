import os
import io
import re
import sqlite3
import boto3
import pandas as pd
import plotly.express as px
import streamlit as st
from database import DatabaseManager
from agent import NLQueryAgent

# ==============================================================================
# 1. STREAMLIT PAGE CONFIGURATION
# ==============================================================================
st.set_page_config(
    page_title="NL Query Agent",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ==============================================================================
# 2. RESILIENT CREDENTIALS & AWS CONFIGURATION (HANDLED IN BACKGROUND)
# ==============================================================================
def get_secret(key: str, default=None):
    """Safely retrieves keys without raising StreamlitSecretNotFoundError."""
    try:
        return st.secrets.get(key, os.getenv(key, default))
    except Exception:
        return os.getenv(key, default)

aws_key = get_secret("AWS_ACCESS_KEY_ID")
aws_secret = get_secret("AWS_SECRET_ACCESS_KEY")
aws_region = get_secret("AWS_DEFAULT_REGION", "us-east-1")
model_choice = "amazon.nova-pro-v1:0"
connection_uri = "sqlite:///data/ecommerce.db"
db_path = os.path.abspath("data/ecommerce.db")

# Verify AWS identity (supports ECS Task Roles and local credentials silently)
try:
    session = boto3.Session(region_name=aws_region)
    has_credentials = (session.get_credentials() is not None) or bool(aws_key and aws_secret)
except Exception:
    has_credentials = False

# ==============================================================================
# 3. DATA INGESTION ENGINE (CSV, Excel, Parquet, JSON, SQLite)
# ==============================================================================
def ingest_uploaded_file(uploaded_file, target_db_path: str) -> tuple[bool, str]:
    """Parses multiple data formats directly into the active SQLite database."""
    fname = uploaded_file.name
    tbl_name = re.sub(r'[^a-zA-Z0-9_]', '_', fname.rsplit('.', 1)[0].lower())
    os.makedirs(os.path.dirname(target_db_path), exist_ok=True)

    try:
        # 1. Direct SQLite database replacement
        if fname.endswith(('.db', '.sqlite', '.sqlite3')):
            with open(target_db_path, "wb") as f:
                f.write(uploaded_file.getbuffer())
            return True, f"Replaced active database with `{fname}`."

        # 2. Delimited text files (CSV / TSV)
        elif fname.endswith('.csv'):
            df = pd.read_csv(uploaded_file)
        elif fname.endswith('.tsv'):
            df = pd.read_csv(uploaded_file, sep='\t')

        # 3. Excel Spreadsheets (multi-sheet support)
        elif fname.endswith(('.xlsx', '.xls')):
            excel_file = pd.ExcelFile(uploaded_file)
            with sqlite3.connect(target_db_path) as conn:
                for sheet in excel_file.sheet_names:
                    sheet_df = pd.read_excel(excel_file, sheet_name=sheet)
                    s_tbl = f"{tbl_name}_{re.sub(r'[^a-zA-Z0-9_]', '_', sheet.lower())}" if len(excel_file.sheet_names) > 1 else tbl_name
                    sheet_df.to_sql(s_tbl, conn, if_exists="replace", index=False)
            return True, f"Ingested {len(excel_file.sheet_names)} sheet(s) into database from `{fname}`."

        # 4. Parquet columnar
        elif fname.endswith('.parquet'):
            df = pd.read_parquet(uploaded_file)

        # 5. JSON / JSON Lines
        elif fname.endswith(('.json', '.jsonl')):
            try:
                df = pd.read_json(uploaded_file)
            except ValueError:
                uploaded_file.seek(0)
                df = pd.read_json(uploaded_file, lines=True)
        else:
            return False, f"Unsupported file type: {fname}"

        # Write DataFrame to SQLite
        with sqlite3.connect(target_db_path) as conn:
            df.to_sql(tbl_name, conn, if_exists="replace", index=False)

        return True, f"Created table `{tbl_name}` ({len(df):,} rows, {len(df.columns)} columns)."

    except Exception as err:
        return False, f"Ingestion failed: {str(err)}"

# In-memory export helpers
@st.cache_data
def convert_df_to_csv(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False).encode("utf-8")

@st.cache_data
def convert_df_to_excel(df: pd.DataFrame) -> bytes:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Query_Results")
    return output.getvalue()

# ==============================================================================
# 4. RESOURCE CACHING
# ==============================================================================
@st.cache_resource(ttl=3600)
def get_db_manager(uri: str):
    return DatabaseManager(connection_uri=uri)

@st.cache_resource(show_spinner="Initializing Bedrock Agent Pipeline...")
def get_agent(key: str, secret: str, region: str, model: str, uri: str):
    return NLQueryAgent(
        aws_access_key=key if key else None,
        aws_secret_key=secret if secret else None,
        aws_region=region,
        model=model,
        db_manager=get_db_manager(uri)
    )

try:
    db_manager = get_db_manager(connection_uri)
except Exception as e:
    st.error(f"Database Connection Failed: {e}")
    st.stop()

# ==============================================================================
# 5. CLEAN SIDEBAR: INGESTION & CONTROLS ONLY
# ==============================================================================
with st.sidebar:
    st.subheader("📂 Data Ingestion")
    uploaded_file = st.file_uploader(
        "Upload dataset",
        type=["csv", "tsv", "xlsx", "xls", "parquet", "json", "jsonl", "db", "sqlite", "sqlite3"],
        help="Upload tabular datasets or an existing SQLite database."
    )
    if uploaded_file is not None:
        if st.button("Load Dataset", use_container_width=True):
            with st.spinner("Processing and indexing data..."):
                success, msg = ingest_uploaded_file(uploaded_file, db_path)
                if success:
                    st.success(msg)
                    st.cache_resource.clear()
                    st.rerun()
                else:
                    st.error(msg)

    st.markdown("---")
    with st.expander("Inspect Database Schema", expanded=False):
        st.code(db_manager.get_schema(), language="sql")

    if st.button("🗑️ Reset Chat History", use_container_width=True):
        st.session_state.messages = []
        st.rerun()

# ==============================================================================
# 6. MAIN CHAT INTERFACE
# ==============================================================================
st.title("Natural Language Database Query Agent")
st.caption("Powered by Amazon Nova Pro & Bedrock • Autonomous Text-to-SQL Engine")

if "messages" not in st.session_state:
    st.session_state.messages = []

# Replay conversation history
for idx, msg in enumerate(st.session_state.messages):
    with st.chat_message(msg["role"]):
        if msg["role"] == "user":
            st.write(msg["content"])
        elif msg["role"] == "assistant":
            st.markdown("**Generated SQL:**")
            st.code(msg["sql"], language="sql")

            df = msg["df"]
            if not df.empty:
                col_left, col_right = st.columns([1, 1])
                with col_left:
                    st.dataframe(df, use_container_width=True)
                    bcol1, bcol2 = st.columns(2)
                    with bcol1:
                        st.download_button("📄 CSV", convert_df_to_csv(df), f"export_{idx}.csv", "text/csv", key=f"csv_{idx}", use_container_width=True)
                    with bcol2:
                        st.download_button("📊 Excel", convert_df_to_excel(df), f"export_{idx}.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key=f"xlsx_{idx}", use_container_width=True)

                with col_right:
                    num_cols = df.select_dtypes(include=["number"]).columns.tolist()
                    cat_cols = df.select_dtypes(include=["object"]).columns.tolist()
                    if cat_cols and num_cols:
                        fig = px.bar(df, x=cat_cols[0], y=num_cols[0], title="Metric Breakdown")
                        st.plotly_chart(fig, use_container_width=True, key=f"chart_{idx}")

            st.markdown(f"**Insight:** {msg['summary']}")

            if msg.get("index_rec"):
                rec = msg["index_rec"]
                st.info(f"💡 **Database Tuning Alert: Unindexed Scans on `{rec['table']}`**")
                with st.expander(f"View Recommended DDL for `{rec['table']}`"):
                    st.markdown(f"**Reasoning:** {rec['reasoning']}")
                    st.code(rec["ddl"], language="sql")

# User Input & Execution Pipeline
user_query = st.chat_input("Ask a question about your database...")

if user_query:
    if not has_credentials:
        st.error("AWS Credentials not detected. Ensure the ECS task role or local environment is configured.")
        st.stop()

    agent = get_agent(aws_key, aws_secret, aws_region, model_choice, connection_uri)
    st.chat_message("user").write(user_query)

    history_turns = [
        {"question": m["prompt_ref"], "sql": m["sql"]}
        for m in st.session_state.messages if m["role"] == "assistant" and "sql" in m
    ]

    with st.chat_message("assistant"):
        with st.status("Agent reasoning & Bedrock execution pipeline...", expanded=True) as status_box:
            try:
                df, final_sql, trace, examples, index_rec = agent.run_query_with_self_healing(
                    natural_query=user_query,
                    history=history_turns,
                    max_retries=3
                )

                st.write(f"🔍 **Matched {len(examples)} reference SQL patterns via Titan vector search**")
                for step in trace:
                    if step["status"] == "success":
                        st.write(f"✅ **Attempt {step['attempt']} verified and executed**")
                    elif step["status"] == "cost_exceeded":
                        st.write(f"🛑 **Attempt {step['attempt']} rejected by cost optimizer:** `{step['error']}`")
                    else:
                        st.write(f"⚠️ **Attempt {step['attempt']} corrected:** `{step['error']}`")

                status_box.update(label="Query executed successfully!", state="complete", expanded=False)

            except Exception as e:
                status_box.update(label="Execution loop failed", state="error", expanded=True)
                st.error(f"Error: {e}")
                st.stop()

        st.markdown("**Generated SQL:**")
        st.code(final_sql, language="sql")

        curr_idx = len(st.session_state.messages)
        if not df.empty:
            col_left, col_right = st.columns([1, 1])
            with col_left:
                st.dataframe(df, use_container_width=True)
                bcol1, bcol2 = st.columns(2)
                with bcol1:
                    st.download_button("📄 CSV", convert_df_to_csv(df), f"export_{curr_idx}.csv", "text/csv", key="csv_live", use_container_width=True)
                with bcol2:
                    st.download_button("📊 Excel", convert_df_to_excel(df), f"export_{curr_idx}.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="xlsx_live", use_container_width=True)

            with col_right:
                num_cols = df.select_dtypes(include=["number"]).columns.tolist()
                cat_cols = df.select_dtypes(include=["object"]).columns.tolist()
                if cat_cols and num_cols:
                    fig = px.bar(df, x=cat_cols[0], y=num_cols[0], title="Metric Breakdown")
                    st.plotly_chart(fig, use_container_width=True, key="chart_live")

        with st.spinner("Synthesizing answer..."):
            summary = agent.summarize_results(user_query, final_sql, df.head(10).to_string())
            st.markdown(f"**Insight:** {summary}")

        if index_rec:
            st.info(f"💡 **Database Tuning Alert: Unindexed Scans on `{index_rec['table']}`**")
            with st.expander(f"View Recommended DDL for `{index_rec['table']}`"):
                st.markdown(f"**Reasoning:** {index_rec['reasoning']}")
                st.code(index_rec["ddl"], language="sql")

    # Persist the full turn in state
    st.session_state.messages.append({"role": "user", "content": user_query})
    st.session_state.messages.append({
        "role": "assistant",
        "prompt_ref": user_query,
        "sql": final_sql,
        "df": df,
        "summary": summary,
        "index_rec": index_rec
    })