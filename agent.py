import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import boto3
import pandas as pd
import yaml

from database import (
    QueryCostExceededError,
    is_read_only_query,
    validate_no_select_star,
    validate_sql_query,
)

try:
    from example_store import ExampleStore
except ImportError:
    class ExampleStore:
        def __init__(self, *args, **kwargs):
            pass
        def get_examples(self, *args, **kwargs) -> List[Dict[str, str]]:
            return []

try:
    from index_advisor import IndexAdvisor
except ImportError:
    class IndexAdvisor:
        def __init__(self, *args, **kwargs):
            pass
        def get_advice(self, sql: str, df: Optional[pd.DataFrame] = None) -> Optional[str]:
            return None

# Retroactive class monkey-patching for seamless backwards compatibility
def _safe_examples_compat(self, *args, **kwargs):
    for name in ["get_examples", "get_similar_examples", "search", "get_similar", "find_similar"]:
        fn = getattr(self, name, None)
        if callable(fn) and fn != _safe_examples_compat:
            try:
                return fn(*args, **kwargs)
            except Exception:
                pass
    if hasattr(self, "examples") and isinstance(self.examples, list):
        return self.examples[:3]
    return []

ExampleStore.get_similar_examples = _safe_examples_compat
ExampleStore.get_examples = _safe_examples_compat

def _safe_advisor_compat(self, *args, **kwargs):
    for name in ["get_advice", "advise", "recommend", "analyze"]:
        fn = getattr(self, name, None)
        if callable(fn) and fn != _safe_advisor_compat:
            try:
                return fn(*args, **kwargs)
            except Exception:
                pass
    return None

IndexAdvisor.get_advice = _safe_advisor_compat


def get_semantic_context() -> str:
    """Reads business formulas, metrics, and synonyms from metrics.yaml wrapped in XML tags."""
    metric_path = Path("metrics.yaml")
    if not metric_path.exists():
        return ""
    try:
        with open(metric_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
        return f"""
<business_semantic_layer>
Use these metric formulas and filters when answering business inquiries:
{yaml.dump(data, default_flow_style=False)}
</business_semantic_layer>
"""
    except Exception:
        return ""


def clean_sql_output(raw_output: str) -> str:
    """Extracts raw SQL statements, removing markdown code fences and trailing semicolons."""
    sql = raw_output.strip()
    match = re.search(r"```(?:sql|trino|presto)?\s*(.*?)\s*```", sql, re.DOTALL | re.IGNORECASE)
    if match:
        sql = match.group(1).strip()
    sql = re.sub(r";+\s*$", "", sql)
    return sql


class NLQueryAgent:
    """Autonomous Text-to-SQL & FinOps Analytics agent powered by Amazon Nova Pro and Amazon Athena."""

    def __init__(
        self,
        aws_access_key: Optional[str] = None,
        aws_secret_key: Optional[str] = None,
        aws_region: str = "us-east-1",
        model: str = "amazon.nova-pro-v1:0",
        db_manager: Any = None
    ):
        self.aws_region = aws_region
        self.model = model
        self.db_manager = db_manager

        session_kwargs = {"region_name": aws_region}
        if aws_access_key and aws_secret_key:
            session_kwargs["aws_access_key_id"] = aws_access_key
            session_kwargs["aws_secret_access_key"] = aws_secret_key

        session = boto3.Session(**session_kwargs)
        self.bedrock = session.client("bedrock-runtime")

        try:
            self.example_store = ExampleStore(self.bedrock)
        except Exception:
            try:
                self.example_store = ExampleStore()
            except Exception:
                self.example_store = None

        try:
            self.advisor = IndexAdvisor(self.bedrock)
        except Exception:
            try:
                self.advisor = IndexAdvisor()
            except Exception:
                self.advisor = None

    def _get_examples(self, query: str) -> List[Dict[str, str]]:
        if not hasattr(self, "example_store") or not self.example_store:
            return []
        try:
            return self.example_store.get_similar_examples(query)
        except Exception:
            return []

    def _get_index_advice(self, sql: str, df: Optional[pd.DataFrame] = None) -> Optional[str]:
        if not hasattr(self, "advisor") or not self.advisor:
            return None
        try:
            return self.advisor.get_advice(sql, df)
        except Exception:
            return None

    def _build_system_prompt(self, schema: str, examples: List[Dict[str, str]]) -> str:
        prompt = (
            "You are an expert enterprise Data Architect and Presto/Trino SQL engineer for Amazon Athena.\n"
            "Your objective is to translate natural language inquiries into highly optimized, syntactically correct Trino SQL queries.\n\n"
            "STRICT EXECUTION GUIDELINES:\n"
            "1. Output ONLY valid Trino/Presto SQL. Do not include conversational commentary or markdown outside the code block.\n"
            "2. Read-Only: Only write SELECT queries or WITH common table expressions followed by SELECT. Never generate DDL or DML mutations.\n"
            "3. Do not append semicolons (;) at the end of the query.\n"
            "4. Cross-Table Joins: Inspect foreign keys in the schema carefully. Always qualify columns with explicit table aliases (e.g. c.customer_id).\n"
            "5. Dialect Rules: Use DATE 'YYYY-MM-DD', DATE_ADD, DATE_DIFF, or INTERVAL arithmetic. Avoid Postgres-specific operators like '::' or 'DISTINCT ON'.\n"
            "6. Handle NULL values gracefully using COALESCE or NULLIF.\n"
            "7. FinOps Column Projection Hygiene: NEVER use 'SELECT *' or 'SELECT table.*' in outer queries, CTEs, or subqueries. Always project only the specific columns required.\n"
            "8. AWS Cost & Usage Report (CUR) Semantic Rules:\n"
            "   - Filtering AWS Services (e.g., EC2, S3, RDS, ELB, CloudWatch):\n"
            "     ALWAYS filter using 'line_item_product_code' (e.g. 'AmazonEC2', 'AmazonS3', 'AmazonRDS', 'AWSELB')\n"
            "     OR 'product_product_name' (e.g. 'Amazon Elastic Compute Cloud', 'Amazon Simple Storage Service').\n"
            "   - NEVER filter 'product_instance_type' with service names like 'EC2%' or 'S3%'. That column only holds server sizes (e.g. 'm6i.large') and is NULL for storage/networking.\n"
            "   - For spend/cost: Use 'line_item_unblended_cost'.\n"
            "   - For daily spend trends or spikes: Group or filter by SUBSTR(line_item_usage_start_date, 1, 10).\n"
            "   - Verify column semantics independently for each query rather than blindly reusing column filters from prior turns.\n\n"
            f"ACTIVE ATHENA / GLUE DATABASE SCHEMAS:\n{schema}\n"
        )

        semantic_context = get_semantic_context()
        if semantic_context:
            prompt += f"\n{semantic_context}\n"

        if examples:
            prompt += "\nREFERENCE GOLDEN SQL EXAMPLES:\n"
            for eg in examples:
                if isinstance(eg, dict):
                    q = eg.get("question") or eg.get("query", "")
                    s = eg.get("sql", "")
                else:
                    q = getattr(eg, "question", "") or getattr(eg, "query", "")
                    s = getattr(eg, "sql", "") or str(eg)
                if q or s:
                    prompt += f"- Question: {q}\n  SQL: {s}\n"

        return prompt

    def _call_bedrock(self, system_prompt: str, messages: List[Dict[str, Any]]) -> str:
        """Invokes Bedrock using the Converse API."""
        try:
            response = self.bedrock.converse(
                modelId=self.model,
                messages=messages,
                system=[{"text": system_prompt}],
                inferenceConfig={"temperature": 0.0, "maxTokens": 2048}
            )
            return response["output"]["message"]["content"][0]["text"]
        except Exception as e:
            raise RuntimeError(f"Bedrock Converse API call failed: {e}")

    def run_query_with_self_healing(
        self,
        natural_query: str,
        history: Optional[List[Dict[str, str]]] = None,
        max_retries: int = 3
    ) -> Tuple[pd.DataFrame, str, List[Dict[str, Any]], List[Dict[str, str]], Optional[str]]:
        """Synthesizes SQL, enforces AST security/cost guardrails, executes against Athena, and repairs retryable errors."""
        schema = self.db_manager.get_schema()
        examples = self._get_examples(natural_query)
        system_prompt = self._build_system_prompt(schema, examples)
        target_dialect = getattr(self.db_manager, "dialect", "trino")

        messages: List[Dict[str, Any]] = []

        if history:
            for turn in history[-3:]:
                messages.append({"role": "user", "content": [{"text": turn["question"]}]})
                messages.append({"role": "assistant", "content": [{"text": turn["sql"]}]})

        messages.append({"role": "user", "content": [{"text": natural_query}]})

        trace: List[Dict[str, Any]] = []
        final_sql = ""
        df = pd.DataFrame()
        last_error = ""

        for attempt in range(1, max_retries + 1):
            raw_response = self._call_bedrock(system_prompt, messages)
            sql = clean_sql_output(raw_response)
            final_sql = sql

            try:
                # 1. Authoritative AST Validation:
                #    - Raises PermissionError for non-SELECT / DDL / DML / multi-statement (non-retryable)
                #    - Raises ValueError / QueryCostExceededError for SELECT * or syntax errors (retryable)
                validate_sql_query(sql, dialect=target_dialect)

                # 2. Database / Athena Execution (also enforces validate_sql_query internally)
                exec_result = self.db_manager.execute_query(sql)
                df = exec_result[0] if isinstance(exec_result, tuple) else exec_result

                # 3. Zero-Result Self-Healing Guardrail
                if (df is None or df.empty) and attempt < max_retries:
                    refinement_hint = (
                        "The executed query returned 0 rows. Please verify your WHERE clause filters against the database schema:\n"
                        "- If filtering for AWS services (EC2, S3, RDS, etc.), use line_item_product_code IN ('AmazonEC2', 'AmazonS3') or product_product_name, NEVER product_instance_type.\n"
                        "- Ensure column values and string patterns match standard Athena conventions.\n"
                        "Provide a corrected Presto/Trino SQL query that accurately filters and returns data."
                    )
                    trace.append({"attempt": attempt, "status": "empty_retry", "error": "Query returned 0 rows. Retrying with corrected schema filters."})
                    messages.append({"role": "assistant", "content": [{"text": sql}]})
                    messages.append({"role": "user", "content": [{"text": refinement_hint}]})
                    continue

                trace.append({"attempt": attempt, "status": "success", "error": None})
                break

            except PermissionError as sec_err:
                # Hard security stop: never retry destructive or multi-statement SQL
                trace.append({"attempt": attempt, "status": "security_blocked", "error": str(sec_err)})
                raise

            except Exception as e:
                last_error = str(e)
                status_label = "cost_guardrail_retry" if isinstance(e, ValueError) else "error"
                trace.append({"attempt": attempt, "status": status_label, "error": last_error})

                if attempt == max_retries:
                    raise RuntimeError(f"Execution failed after {max_retries} attempts. Last error: {last_error}") from e

                messages.append({"role": "assistant", "content": [{"text": sql}]})
                correction_prompt = (
                    f"Attempt {attempt} failed with the following error:\n{last_error}\n\n"
                    "Analyze the error trace, inspect the column names and schemas carefully, "
                    "explicitly project only the required columns (never use SELECT * in any query, CTE, or subquery), "
                    "and provide a corrected Presto/Trino SQL query with no conversational markdown."
                )
                messages.append({"role": "user", "content": [{"text": correction_prompt}]})

        index_rec = self._get_index_advice(final_sql, df)
        return df, final_sql, trace, examples, index_rec

    def summarize_results(self, natural_query: str, sql: str, df_preview: str) -> str:
        """Synthesizes executive insights, root cause analysis, and actionable FinOps optimization guidance."""
        system_instruction = (
            "You are a Principal Cloud FinOps Architect and AWS Cost Optimization Specialist. "
            "Your role is to analyze cloud billing and usage data, explain the underlying technical drivers, "
            "and provide decisive, high-impact AWS cost optimization advice."
        )

        finops_prompt = f"""User Question: {natural_query}
Executed Athena SQL: {sql}
Query Results Preview:
{df_preview}

Please provide a structured, executive-ready response adhering to this format:

### 📊 Spend & Trend Analysis
- State the direct answer to the user inquiry with exact dollar amounts, dates, and percentages.
- Identify any noticeable spikes, trends, or cost concentrations.

### 🔍 Root Cause Diagnosis
- Explain the technical reasons behind these numbers (e.g., month-start commitment fee amortization, snapshot scheduling, untagged non-production compute, oversized database instances, or end-of-month batch pipelines).

### 💡 FinOps Recommendations & Cost Optimization
Provide 2 to 4 concrete, actionable AWS engineering and procurement fixes:
1. **Immediate Waste Elimination**: (e.g., stopping non-prod environments outside business hours via AWS Instance Scheduler, deleting unattached EBS volumes or obsolete snapshots).
2. **Commitment & Pricing Model Tuning**: (e.g., Compute Savings Plans, EC2 Instance Savings Plans, or transitioning On-Demand instances to Spot for fault-tolerant workers).
3. **Architectural Rightsizing & Modernization**: (e.g., downsizing overprovisioned instance types, migrating from x86 to AWS Graviton processors, migrating GP2 to GP3 EBS volumes).
4. **Governance & Anomaly Alerting**: (e.g., configuring AWS Cost Anomaly Detection with SNS alerts, enforcing tag-or-terminate policies).
"""

        try:
            response = self.bedrock.converse(
                modelId=self.model,
                messages=[{"role": "user", "content": [{"text": finops_prompt}]}],
                system=[{"text": system_instruction}],
                inferenceConfig={"temperature": 0.2, "maxTokens": 2048}
            )
            return response["output"]["message"]["content"][0]["text"].strip()
        except Exception as e:
            return f"Analysis completed. (FinOps advisory synthesis unavailable: {e})"
