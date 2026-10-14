import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import boto3
import pandas as pd
import sqlglot
from sqlglot import exp
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
    """
    Strictly extracts a single SQL statement from model output.
    Rejects multiple fenced code blocks or conversational text surrounding a code block
    with a retryable ValueError rather than silently guessing which block to execute.
    """
    if not raw_output or not raw_output.strip():
        raise ValueError("Model returned an empty SQL response.")

    text = raw_output.strip()
    fenced_blocks = re.findall(
        r"```(?:sql|trino|presto|sqlite|postgres|mysql)?\s*(.*?)\s*```",
        text,
        flags=re.DOTALL | re.IGNORECASE,
    )

    if len(fenced_blocks) > 1:
        raise ValueError(
            "Ambiguous model output: multiple markdown code blocks were returned. "
            "Output ONLY a single valid SQL statement."
        )

    if len(fenced_blocks) == 1:
        outside_text = re.sub(
            r"```(?:sql|trino|presto|sqlite|postgres|mysql)?\s*.*?\s*```",
            "",
            text,
            flags=re.DOTALL | re.IGNORECASE,
        ).strip()
        if outside_text:
            raise ValueError(
                "Model output included conversational prose outside the SQL code block. "
                "Output ONLY the raw SQL query."
            )
        sql = fenced_blocks[0].strip()
    else:
        sql = text

    sql = re.sub(r";+\s*$", "", sql)
    return sql


def _has_suspicious_cur_filter(sql: str, dialect: str = "trino") -> bool:
    """
    Inspects the SQL AST to determine whether an empty result was likely caused by
    filtering 'product_instance_type' with a service name (e.g. 'EC2', 'S3', 'RDS')
    rather than 'line_item_product_code'. Avoids retrying valid queries that
    legitimately return 0 rows.
    """
    try:
        ast = sqlglot.parse_one(sql, read=dialect)
        for col in ast.find_all(exp.Column):
            if col.name.lower() == "product_instance_type":
                parent_pred = col.find_ancestor(exp.EQ, exp.Like, exp.ILike, exp.In)
                if parent_pred:
                    pred_sql = parent_pred.sql().upper()
                    if any(svc in pred_sql for svc in ("EC2", "S3", "RDS", "LAMBDA", "ELB", "CLOUDWATCH")):
                        return True
    except Exception:
        pass
    return False


class NLQueryAgent:
    """Autonomous Text-to-SQL & FinOps Analytics agent powered by Amazon Nova Pro and Amazon Athena."""

    def __init__(
        self,
        aws_access_key: Optional[str] = None,
        aws_secret_key: Optional[str] = None,
        aws_region: str = "us-east-1",
        model: str = "amazon.nova-pro-v1:0",
        db_manager: Any = None,
        sql_dialect: Optional[str] = None,
    ):
        self.aws_region = aws_region
        self.model = model
        self.db_manager = db_manager
        # Separate SQL generator dialect from local test DB unless explicitly overridden
        self.sql_dialect = sql_dialect or getattr(db_manager, "dialect", "trino") or "trino"

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
        """Retrieves few-shot SQL examples without class-level monkey-patching."""
        if not self.example_store:
            return []

        for method_name in (
            "get_similar_examples",
            "get_examples",
            "search",
            "get_similar",
            "find_similar",
        ):
            method = getattr(self.example_store, method_name, None)
            if callable(method):
                try:
                    result = method(query)
                    if result is not None:
                        return result
                except Exception:
                    continue

        examples_attr = getattr(self.example_store, "examples", None)
        if isinstance(examples_attr, list):
            return examples_attr[:3]
        return []

    def _get_index_advice(self, sql: str, df: Optional[pd.DataFrame] = None) -> Optional[str]:
        """Retrieves index/partition recommendations without class-level monkey-patching."""
        if not self.advisor:
            return None

        for method_name in ("get_advice", "advise", "recommend", "analyze"):
            method = getattr(self.advisor, method_name, None)
            if callable(method):
                try:
                    result = method(sql, df)
                    if result is not None:
                        return result
                except TypeError:
                    try:
                        result = method(sql)
                        if result is not None:
                            return result
                    except Exception:
                        continue
                except Exception:
                    continue
        return None

    def _build_system_prompt(self, schema: str, examples: List[Dict[str, str]]) -> str:
        prompt = (
            "You are an expert enterprise Data Architect and Presto/Trino SQL engineer for Amazon Athena.\n"
            "Your objective is to translate natural language inquiries into highly optimized, syntactically correct SQL queries.\n\n"
            "STRICT EXECUTION GUIDELINES:\n"
            "1. Output ONLY a single valid SQL statement. Do not include conversational commentary or multiple code blocks.\n"
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

    @staticmethod
    def _build_conversation_messages(
        natural_query: str, history: Optional[List[Dict[str, str]]] = None
    ) -> List[Dict[str, Any]]:
        """
        Builds a strictly alternating user/assistant message sequence for Bedrock Converse,
        labeling prior SQL turns as historical context.
        """
        messages: List[Dict[str, Any]] = []
        if history:
            for turn in history[-3:]:
                q = (turn.get("question") or "").strip()
                s = (turn.get("sql") or "").strip()
                if q and s:
                    messages.append({"role": "user", "content": [{"text": q}]})
                    messages.append(
                        {
                            "role": "assistant",
                            "content": [{"text": f"-- Previous generated SQL:\n{s}"}],
                        }
                    )

        messages.append({"role": "user", "content": [{"text": natural_query}]})
        return messages

    def _call_bedrock(self, system_prompt: str, messages: List[Dict[str, Any]]) -> str:
        """Validates message alternation and invokes Bedrock using the Converse API."""
        for idx, msg in enumerate(messages):
            expected_role = "user" if idx % 2 == 0 else "assistant"
            if msg.get("role") != expected_role:
                raise ValueError(
                    f"Invalid Converse message sequence at index {idx}: expected '{expected_role}', got '{msg.get('role')}'."
                )

        try:
            response = self.bedrock.converse(
                modelId=self.model,
                messages=messages,
                system=[{"text": system_prompt}],
                inferenceConfig={"temperature": 0.0, "maxTokens": 2048}
            )
            return response["output"]["message"]["content"][0]["text"]
        except Exception as e:
            raise RuntimeError(f"Bedrock Converse API call failed: {e}") from e

    def run_query_with_self_healing(
        self,
        natural_query: str,
        history: Optional[List[Dict[str, str]]] = None,
        max_retries: int = 3
    ) -> Tuple[pd.DataFrame, str, List[Dict[str, Any]], List[Dict[str, str]], Optional[str]]:
        """
        Synthesizes SQL, enforces AST security/cost/syntax guardrails, executes against Athena,
        and repairs retryable SQL errors without retrying AWS infrastructure failures.
        """
        schema = self.db_manager.get_schema()
        examples = self._get_examples(natural_query)
        system_prompt = self._build_system_prompt(schema, examples)
        execution_dialect = getattr(self.db_manager, "dialect", self.sql_dialect) or "trino"

        messages = self._build_conversation_messages(natural_query, history)

        trace: List[Dict[str, Any]] = []
        final_sql = ""
        df = pd.DataFrame()
        last_error = ""

        for attempt in range(1, max_retries + 1):
            raw_response = self._call_bedrock(system_prompt, messages)

            try:
                sql = clean_sql_output(raw_response)
                final_sql = sql

                # 1. Validate SQL AST (Security -> PermissionError, Wildcard -> QueryCostExceededError, Syntax -> ValueError)
                validate_sql_query(sql, dialect=execution_dialect)

                # 2. Execute against target backend
                exec_result = self.db_manager.execute_query(sql)
                df = exec_result[0] if isinstance(exec_result, tuple) else exec_result

                # 3. Only retry an empty result if the AST exhibits a known CUR column filter error
                if (df is None or df.empty) and attempt < max_retries and _has_suspicious_cur_filter(sql, dialect=execution_dialect):
                    refinement_hint = (
                        "The executed query returned 0 rows because 'product_instance_type' was filtered with an AWS service name. "
                        "Use line_item_product_code (e.g. 'AmazonEC2', 'AmazonS3') or product_product_name instead, "
                        "and return a single corrected SQL query."
                    )
                    trace.append({
                        "attempt": attempt,
                        "status": "empty_filter_retry",
                        "error": "Suspicious service filter on product_instance_type returned 0 rows."
                    })
                    messages.append({"role": "assistant", "content": [{"text": sql}]})
                    messages.append({"role": "user", "content": [{"text": refinement_hint}]})
                    continue

                trace.append({"attempt": attempt, "status": "success", "error": None})
                break

            except PermissionError as sec_err:
                trace.append({
                    "attempt": attempt,
                    "status": "security_blocked",
                    "error": str(sec_err)
                })
                raise

            except QueryCostExceededError as cost_err:
                last_error = str(cost_err)
                trace.append({
                    "attempt": attempt,
                    "status": "cost_guardrail_retry",
                    "error": last_error
                })

            except ValueError as validation_err:
                last_error = str(validation_err)
                trace.append({
                    "attempt": attempt,
                    "status": "sql_validation_retry",
                    "error": last_error
                })

            except Exception as e:
                last_error = str(e)
                trace.append({
                    "attempt": attempt,
                    "status": "execution_error",
                    "error": last_error
                })
                raise

            # Common retry logic reached ONLY for QueryCostExceededError and ValueError
            if attempt == max_retries:
                raise RuntimeError(
                    f"SQL generation failed after {max_retries} attempts. Last validation/cost error: {last_error}"
                )

            assistant_turn_text = final_sql if final_sql else raw_response.strip()
            messages.append({"role": "assistant", "content": [{"text": assistant_turn_text}]})
            correction_prompt = (
                f"Attempt {attempt} failed validation with the following error:\n{last_error}\n\n"
                "Inspect the schema and error message carefully, explicitly project only required columns "
                "(never use SELECT * in any query, CTE, or subquery), and return ONLY a single corrected SQL query."
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
