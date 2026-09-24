import os
import io
import re
import boto3
import pandas as pd
import plotly.express as px
import streamlit as st
from athena_manager import AthenaManager
from agent import NLQueryAgent

# ==============================================================================
# 1. STREAMLIT PAGE CONFIGURATION & STYLING
# ==============================================================================
st.set_page_config(
    page_title="NL Query Agent (Athena Serverless Lake)",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Text-wrapping overrides for file uploader and cached query history buttons
st.markdown(
    """
    <style>
    [data-testid="stFileUploader"] small, 
    [data-testid="stFileUploaderDropzoneInstructions"] div {
        white-space: normal !important;
        word-break: break-word !important;
        font-size: 0.75rem !important;
        line-height: 1.3 !important;
    }
    button[key^="prev_req_"] {
        text-align: left !important;
        justify-content: flex-start !important;
        border-radius: 6px !important;
        font-size: 0.82rem !important;
        padding: 4px 8px !important;
    }
    </style>
    """,
    unsafe_allow_html=True
)

# ==============================================================================
# 2. CREDENTIALS & ATHENA DATA LAKE CONFIGURATION
# ==============================================================================
def get_secret(key: str, default=None):
    """Safely retrieves secrets without raising StreamlitSecretNotFoundError."""
    try:
        return st.secrets.get(key, os.getenv(key, default))
    except Exception:
        return os.getenv(key, default)

aws_key = get_secret("AWS_ACCESS_KEY_ID")
aws_secret = get_secret("AWS_SECRET_ACCESS_KEY")
aws_region = get_secret("AWS_DEFAULT_REGION", "us-east-1")
model_choice = "amazon.nova-pro-v1:0"

# Auto-discover AWS Account ID for S3 lake bucket naming
try:
    session = boto3.Session(
        aws_access_key_id=aws_key,
        aws_secret_access_key=aws_secret,
        region_name=aws_region
    )
    account_id = session.client("sts").get_caller_identity()["Account"]
    has_credentials = True
except Exception:
    account_id = "default"
    has_credentials = False

BUCKET_NAME = f"query-agent-lake-{account_id}"
DATABASE_NAME = "query_agent_db"

# ==============================================================================
# 3. RESOURCE CACHING & ENGINE INITIALIZATION
# ==============================================================================
@st.cache_resource(ttl=3600)
def get_athena_engine(db_name: str, b_name: str, reg: str):
    return AthenaManager(
        database=db_name,
        bucket_name=b_name,
        region=reg
    )

@st.cache_resource(show_spinner="Initializing Bedrock Nova Pro Agent Pipeline...")
def get_agent(key: str, secret: str, reg: str, model: str, engine):
    return NLQueryAgent(
        aws_access_key=key if key else None,
        aws_secret_key=secret if secret else None,
        aws_region=reg,
        model=model,
        db_manager=engine
    )

# Export Helpers
@st.cache_data
def convert_df_to_csv(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False).encode("utf-8")

@st.cache_data
def convert_df_to_excel(df: pd.DataFrame) -> bytes:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Query_Results")
    return output.getvalue()

try:
    db_manager = get_athena_engine(DATABASE_NAME, BUCKET_NAME, aws_region)
except Exception as e:
    st.error(f"Athena Connection Initialization Failed: {e}")
    st.stop()

# ==============================================================================
# 4. SIDEBAR: DATA INGESTION, SCHEMA & 1-CLICK QUERY CACHE
# ==============================================================================
if "messages" not in st.session_state:
    st.session_state.messages = []

with st.sidebar:
    st.subheader("📂 S3 Data Lake Ingestion")
    uploaded_file = st.file_uploader(
        "Upload dataset directly to S3",
        type=["csv", "tsv", "xlsx", "xls", "parquet"],
        help="Streams file into your S3 Data Lake and registers it as an Athena external table."
    )
    if uploaded_file is not None:
        if st.button("Load Dataset into Lake", use_container_width=True):
            with st.spinner("Streaming file to S3 and registering schema in AWS Glue..."):
                try:
                    msg = db_manager.stream_upload_and_create_table(uploaded_file)
                    st.success(msg)
                    st.cache_resource.clear()
                    st.rerun()
                except Exception as err:
                    st.error(f"Ingestion failed: {err}")

    st.markdown("---")
    with st.expander("Inspect Athena / Glue Schema", expanded=False):
        try:
            st.code(db_manager.get_schema(), language="sql")
        except Exception:
            st.caption("No tables registered in catalog yet.")

    if st.button("🗑️ Reset Chat History", use_container_width=True):
        st.session_state.messages = []
        st.session_state.pop("queued_prompt", None)
        st.rerun()

    # 1-Click Cached Requests List
    st.markdown("---")
    st.subheader("🕒 Previous Requests")
    historical_queries = [
        msg["content"] for msg in st.session_state.messages
        if msg.get("role") == "user"
    ]
    if historical_queries:
        for idx, q_text in enumerate(reversed(historical_queries)):
            display_label = (q_text[:32] + "...") if len(q_text) > 32 else q_text
            if st.button(f"💬 {display_label}", key=f"prev_req_{idx}", use_container_width=True, help=f"Click to run: {q_text}"):
                st.session_state["queued_prompt"] = q_text
                st.rerun()
    else:
        st.caption("No previous requests yet. Queries you submit will appear here.")

# ==============================================================================
# 5. MAIN CHAT INTERFACE & CONVERSATION DISPLAY
# ==============================================================================
st.title("Natural Language Database Query Agent")
st.caption("Powered by Amazon Nova Pro & Bedrock • Serverless S3 & Athena Analytics Engine")

for idx, msg in enumerate(st.session_state.messages):
    with st.chat_message(msg["role"]):
        if msg["role"] == "user":
            st.write(msg["content"])
        elif msg["role"] == "assistant":
            st.markdown("**Generated Athena SQL:**")
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

# ==============================================================================
# 6. USER INPUT & QUERY EXECUTION PIPELINE
# ==============================================================================
queued_prompt = st.session_state.pop("queued_prompt", None)
user_query = st.chat_input("Ask a question about your S3 data lake...") or queued_prompt

if user_query:
    if not has_credentials:
        st.error("AWS Credentials not detected. Ensure the ECS task role or AWS environment is configured.")
        st.stop()

    agent = get_agent(aws_key, aws_secret, aws_region, model_choice, db_manager)
    st.chat_message("user").write(user_query)

    history_turns = [
        {"question": m["prompt_ref"], "sql": m["sql"]}
        for m in st.session_state.messages if m.get("role") == "assistant" and "sql" in m
    ]

    with st.chat_message("assistant"):
        with st.status("Agent reasoning & Athena execution pipeline...", expanded=True) as status_box:
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
                status_box.update(label="Athena execution failed", state="error", expanded=True)
                st.error(f"Error: {e}")
                st.stop()

        st.markdown("**Generated Athena SQL:**")
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