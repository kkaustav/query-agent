import os
import io
import re
import uuid
import boto3
import pandas as pd
import plotly.express as px
import streamlit as st
from athena_manager import AthenaManager
from agent import NLQueryAgent

# ==============================================================================
# 1. STREAMLIT PAGE CONFIGURATION & CSS OVERRIDES
# ==============================================================================
st.set_page_config(
    page_title="NL Query Agent (Athena Serverless Lake)",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Hide Streamlit's default 200MB subtext, style dropzone, and style history links
st.markdown(
    """
    <style>
    /* Suppress Streamlit's default dropzone instruction text and small limit tag */
    [data-testid="stFileUploaderDropzoneInstructions"],
    [data-testid="stFileUploader"] small {
        display: none !important;
    }

    /* Ensure the Upload / Browse button is visible and full-width */
    [data-testid="stFileUploaderDropzone"] {
        padding: 10px !important;
        min-height: unset !important;
        border: 1px dashed rgba(255, 255, 255, 0.25) !important;
        background: transparent !important;
    }
    [data-testid="stFileUploaderDropzone"] button {
        width: 100% !important;
    }

    /* Format & limit specifications badge box */
    .badge-box {
        background-color: rgba(255, 255, 255, 0.05);
        border: 1px solid rgba(255, 255, 255, 0.12);
        border-radius: 6px;
        padding: 8px 12px;
        margin-bottom: 10px;
        font-size: 0.8rem;
        line-height: 1.4;
    }

    /* History links that open in new tabs */
    a.history-tab-link {
        display: block !important;
        text-decoration: none !important;
        color: #e0e0e0 !important;
        background-color: rgba(255, 255, 255, 0.05) !important;
        border: 1px solid rgba(255, 255, 255, 0.1) !important;
        border-radius: 6px !important;
        padding: 7px 10px !important;
        margin-bottom: 6px !important;
        font-size: 0.82rem !important;
        white-space: nowrap !important;
        overflow: hidden !important;
        text-overflow: ellipsis !important;
        transition: all 0.2s ease !important;
    }
    a.history-tab-link:hover {
        background-color: rgba(255, 255, 255, 0.12) !important;
        border-color: rgba(255, 255, 255, 0.25) !important;
        color: #ffffff !important;
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

try:
    session = boto3.Session(
        aws_access_key_id=aws_key,
        aws_secret_access_key=aws_secret,
        region_name=aws_region
    )
    account_id = session.client("sts").get_caller_identity()["Account"]
    has_credentials = True
except Exception:
    account_id = "213649490780"
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
def get_agent(key: str, secret: str, reg: str, model: str, _engine):
    return NLQueryAgent(
        aws_access_key=key if key else None,
        aws_secret_key=secret if secret else None,
        aws_region=reg,
        model=model,
        db_manager=_engine
    )


# Shared in-memory store so newly opened browser tabs can load archived chats
@st.cache_resource
def get_shared_sessions():
    return {}


shared_sessions = get_shared_sessions()


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
# 4. MULTI-TAB SESSION ROUTING VIA URL QUERY PARAMETERS
# ==============================================================================
requested_session_id = st.query_params.get("session")

if "active_session_id" not in st.session_state:
    if requested_session_id and requested_session_id in shared_sessions:
        st.session_state.active_session_id = requested_session_id
        st.session_state.messages = list(shared_sessions[requested_session_id]["messages"])
    else:
        st.session_state.active_session_id = str(uuid.uuid4())[:8]
        st.session_state.messages = []
elif requested_session_id and requested_session_id in shared_sessions and st.session_state.active_session_id != requested_session_id:
    st.session_state.active_session_id = requested_session_id
    st.session_state.messages = list(shared_sessions[requested_session_id]["messages"])

# ==============================================================================
# 5. SIDEBAR: DATA INGESTION, SCHEMA & NEW-TAB HISTORY LINKS
# ==============================================================================
with st.sidebar:
    st.subheader("📂 S3 Data Lake Ingestion")

    st.markdown(
        """
        <div class="badge-box">
            <b>Storage Limit:</b> Up to <b>4 TB</b> per dataset<br/>
            <b>Allowed Formats:</b> CSV, TSV, XLSX, XLS, PARQUET
        </div>
        """,
        unsafe_allow_html=True
    )

    uploaded_file = st.file_uploader(
        "Upload dataset directly to S3",
        type=["csv", "tsv", "xlsx", "xls", "parquet"],
        label_visibility="collapsed"
    )

    if uploaded_file is not None:
        file_signature = f"{uploaded_file.name}_{uploaded_file.size}"
        if st.session_state.get("last_uploaded_file") != file_signature:
            with st.spinner("Streaming file to S3 and registering schema in AWS Glue..."):
                try:
                    msg = db_manager.stream_upload_and_create_table(uploaded_file)
                    st.session_state["last_uploaded_file"] = file_signature
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

    # Reset current session or start a fresh query
    if st.button("➕ New Chat / Reset", use_container_width=True):
        st.session_state.active_session_id = str(uuid.uuid4())[:8]
        st.session_state.messages = []
        st.query_params.clear()
        st.rerun()

    # Browser-Style History: Opens in a New Tab
    st.markdown("---")
    st.subheader("🕒 Previous Chats")
    st.caption("Clicking opens the conversation in a new tab.")

    if shared_sessions:
        for s_id, s_data in reversed(list(shared_sessions.items())):
            is_active = (s_id == st.session_state.active_session_id)
            icon = "🟢 " if is_active else "💬 "
            display_title = f"{icon}{s_data['title']}"

            # HTML Anchor with target="_blank" opens clean in a new tab
            st.markdown(
                f"""
                <a class="history-tab-link" href="/?session={s_id}" target="_blank" title="Open '{s_data['title']}' in new tab">
                    {display_title}
                </a>
                """,
                unsafe_allow_html=True
            )
    else:
        st.caption("No archived chats yet. Submitted queries will appear here.")

# ==============================================================================
# 6. MAIN CHAT INTERFACE & CONVERSATION DISPLAY
# ==============================================================================
st.title("Natural Language Database Query Agent")
st.caption("Powered by Amazon Nova Pro & Bedrock • Serverless S3 & Athena Analytics Engine")

# If currently viewing an archived session from a query param, display an indicator
if requested_session_id and requested_session_id in shared_sessions:
    st.info(f"Viewing archived conversation: **{shared_sessions[requested_session_id]['title']}**")

for idx, msg in enumerate(st.session_state.messages):
    with st.chat_message(msg["role"]):
        if msg["role"] == "user":
            st.write(msg["content"])
        elif msg["role"] == "assistant":
            st.markdown("**Generated Athena SQL:**")
            st.code(msg["sql"], language="sql")

            df = msg.get("df")
            if isinstance(df, pd.DataFrame) and not df.empty:
                col_left, col_right = st.columns([1, 1])
                with col_left:
                    st.dataframe(df, use_container_width=True)
                    bcol1, bcol2 = st.columns(2)
                    with bcol1:
                        st.download_button("📄 CSV", convert_df_to_csv(df), f"export_{idx}.csv", "text/csv",
                                           key=f"csv_{idx}", use_container_width=True)
                    with bcol2:
                        st.download_button("📊 Excel", convert_df_to_excel(df), f"export_{idx}.xlsx",
                                           "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                           key=f"xlsx_{idx}", use_container_width=True)

                with col_right:
                    num_cols = df.select_dtypes(include=["number"]).columns.tolist()
                    cat_cols = df.select_dtypes(include=["object"]).columns.tolist()
                    if cat_cols and num_cols:
                        fig = px.bar(df, x=cat_cols[0], y=num_cols[0], title="Metric Breakdown")
                        st.plotly_chart(fig, use_container_width=True, key=f"chart_{idx}")

            st.markdown(f"**Insight:** {msg['summary']}")

# ==============================================================================
# 7. USER INPUT & QUERY EXECUTION PIPELINE
# ==============================================================================
user_query = st.chat_input("Ask a question about your S3 data lake...")

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
        if isinstance(df, pd.DataFrame) and not df.empty:
            col_left, col_right = st.columns([1, 1])
            with col_left:
                st.dataframe(df, use_container_width=True)
                bcol1, bcol2 = st.columns(2)
                with bcol1:
                    st.download_button("📄 CSV", convert_df_to_csv(df), f"export_{curr_idx}.csv", "text/csv",
                                       key="csv_live", use_container_width=True)
                with bcol2:
                    st.download_button("📊 Excel", convert_df_to_excel(df), f"export_{curr_idx}.xlsx",
                                       "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                       key="xlsx_live", use_container_width=True)

            with col_right:
                num_cols = df.select_dtypes(include=["number"]).columns.tolist()
                cat_cols = df.select_dtypes(include=["object"]).columns.tolist()
                if cat_cols and num_cols:
                    fig = px.bar(df, x=cat_cols[0], y=num_cols[0], title="Metric Breakdown")
                    st.plotly_chart(fig, use_container_width=True, key="chart_live")

        with st.spinner("Synthesizing answer..."):
            df_preview = df.head(10).to_string() if isinstance(df, pd.DataFrame) else str(df)
            summary = agent.summarize_results(user_query, final_sql, df_preview)
            st.markdown(f"**Insight:** {summary}")

    # Append to current session
    st.session_state.messages.append({"role": "user", "content": user_query})
    st.session_state.messages.append({
        "role": "assistant",
        "prompt_ref": user_query,
        "sql": final_sql,
        "df": df,
        "summary": summary,
        "index_rec": index_rec
    })

    # Save immediately into shared multi-tab storage
    curr_id = st.session_state.active_session_id
    active_first_msg = next(
        (m["content"] for m in st.session_state.messages if m.get("role") == "user"),
        user_query
    )
    title = (active_first_msg[:30] + "...") if len(active_first_msg) > 30 else active_first_msg
    shared_sessions[curr_id] = {
        "title": title,
        "messages": list(st.session_state.messages)
    }