import streamlit as st
import os
import io
import boto3
import pandas as pd
import plotly.express as px
from database import DatabaseManager
from agent import NLQueryAgent

st.set_page_config(page_title="NL Query Agent", page_icon="⚡", layout="wide")

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

# Session State for conversation history
if "messages" not in st.session_state:
    st.session_state.messages = []

# ==========================================
# SIDEBAR: AWS BEDROCK & DATABASE SETTINGS
# ==========================================
st.sidebar.title("Agent Controls")

# 1. Inspect Streamlit Secrets / Environment
aws_key = st.secrets.get("AWS_ACCESS_KEY_ID", os.getenv("AWS_ACCESS_KEY_ID", None))
aws_secret = st.secrets.get("AWS_SECRET_ACCESS_KEY", os.getenv("AWS_SECRET_ACCESS_KEY", None))
aws_region = st.secrets.get("AWS_DEFAULT_REGION", os.getenv("AWS_DEFAULT_REGION", "us-east-1"))

# 2. Check if local AWS CLI credentials or secrets exist
try:
    session = boto3.Session(region_name=aws_region)
    has_credentials = (session.get_credentials() is not None) or bool(aws_key and aws_secret)
except Exception:
    has_credentials = False

if has_credentials:
    st.sidebar.success("⚡ AWS Bedrock: Authenticated")
else:
    st.sidebar.warning("No AWS credentials detected.")
    aws_key = st.sidebar.text_input("AWS Access Key ID", type="password")
    aws_secret = st.sidebar.text_input("AWS Secret Access Key", type="password")

aws_region = st.sidebar.selectbox("AWS Region", ["us-east-1", "us-west-2", "ap-south-1"], index=0)

model_choice = st.sidebar.text_input(
    "Bedrock Model ID",
    value="amazon.nova-pro-v1:0"
)

st.sidebar.markdown("---")
st.sidebar.subheader("Database Target")
db_type = st.sidebar.selectbox("Engine Target", ["SQLite (Local Sandbox)", "PostgreSQL", "MySQL"])

if db_type == "SQLite (Local Sandbox)":
    connection_uri = "sqlite:///data/ecommerce.db"
elif db_type == "PostgreSQL":
    connection_uri = st.sidebar.text_input("PostgreSQL URI", placeholder="postgresql+psycopg2://user:pass@host:5432/dbname", type="password")
elif db_type == "MySQL":
    connection_uri = st.sidebar.text_input("MySQL URI", placeholder="mysql+pymysql://user:pass@host:3306/dbname", type="password")

if not connection_uri:
    st.info("Please enter a valid connection string.")
    st.stop()

@st.cache_resource(ttl=3600)
def get_db_manager(uri: str):
    return DatabaseManager(connection_uri=uri)

try:
    db_manager = get_db_manager(connection_uri)
    st.sidebar.success(f"Connected: `{db_manager.dialect.upper()}` Engine")
except Exception as e:
    st.sidebar.error(f"Connection Failed: {e}")
    st.stop()

if st.sidebar.button("🗑️ Reset Chat History", use_container_width=True):
    st.session_state.messages = []
    st.rerun()

with st.sidebar.expander("Inspect Target Schema"):
    st.code(db_manager.get_schema(), language="sql")

@st.cache_resource(show_spinner="Connecting to Bedrock & Pre-Computing Embeddings...")
def get_agent(key: str, secret: str, region: str, model: str, uri: str):
    return NLQueryAgent(
        aws_access_key=key if key else None,
        aws_secret_key=secret if secret else None,
        aws_region=region,
        model=model,
        db_manager=get_db_manager(uri)
    )

# ==========================================
# MAIN INTERFACE & CONVERSATION DISPLAY
# ==========================================
st.title("Natural Language Database Query Agent")
st.caption("Powered by Amazon Nova Pro & Bedrock • Autonomous Text-to-SQL Engine")

# Replay previous chat turns
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

# ==========================================
# USER INPUT & QUERY EXECUTION
# ==========================================
user_query = st.chat_input("Ask a question about sales, products, or customers...")

if user_query:
    if not has_credentials and (not aws_key or not aws_secret):
        st.error("Please provide AWS Credentials in `.streamlit/secrets.toml` or the sidebar.")
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