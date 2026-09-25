import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import boto3
import pandas as pd
import yaml

# Safety and helper imports matching repository architecture
try:
    from database import is_read_only_query
except ImportError:
    def is_read_only_query(sql: str) -> bool:
        forbidden = ["DROP", "DELETE", "INSERT", "UPDATE", "ALTER", "TRUNCATE", "CREATE", "GRANT", "REVOKE"]
        clean = re.sub(r"--.*?\n", "", sql, flags=re.MULTILINE)
        clean = re.sub(r"/\*.*?\*/", "", clean, flags=re.DOTALL).strip().upper()
        tokens = re.findall(r"\b[A-Z]+\b", clean)
        return not any(verb in tokens for verb in forbidden)

try:
    from example_store import ExampleStore
except ImportError:
    class ExampleStore:
        def __init__(self, *args, **kwargs):
            pass
        def get_similar_examples(self, query: str, k: int = 3) -> List[Dict[str, str]]:
            return []

try:
    from index_advisor import IndexAdvisor
except ImportError:
    class IndexAdvisor:
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
    """Extracts raw SQL statements, removing markdown code fences and trailing semicolons."""
    sql = raw_output.strip()
    match = re.search(r"```(?:sql|trino|presto)?\s*(.*?)\s*```", sql, re.DOTALL | re.IGNORECASE)
    if match:
        sql = match.group(1).strip()
    sql = re.sub(r";+\s*$", "", sql)
    return sql


class NLQueryAgent:
    """Autonomous Text-to-SQL agent powered by Amazon Nova Pro and Amazon Athena."""

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

        self.example_store = ExampleStore(
            aws_access_key=aws_access_key,
            aws_secret_key=aws_secret_key,
            aws_region=aws_region
        )
        self.advisor = IndexAdvisor()

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
            "6. Handle NULL values gracefully using COALESCE or NULLIF.\n\n"
            f"ACTIVE ATHENA / GLUE DATABASE SCHEMAS:\n{schema}\n"
        )

        # Inject business semantic layer rules into Bedrock prompt
        semantic_context = get_semantic_context()
        if semantic_context:
            prompt += f"\n{semantic_context}\n"

        if examples:
            prompt += "\nREFERENCE GOLDEN SQL EXAMPLES:\n"
            for eg in examples:
                q = eg.get("question") or eg.get("query", "")
                s = eg.get("sql", "")
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
        """Synthesizes SQL, sanitizes queries via AST, executes against Athena, and repairs errors autonomously."""
        schema = self.db_manager.get_schema()
        examples = self.example_store.get_similar_examples(natural_query, k=3)
        system_prompt = self._build_system_prompt(schema, examples)

        messages: List[Dict[str, Any]] = []

        # Incorporate prior chat turns
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

            # 1. AST Read-Only Sanitization Guardrail
            if not is_read_only_query(sql):
                err_msg = "Security violation: AST query sanitizer rejected non-SELECT or destructive statement."
                trace.append({"attempt": attempt, "status": "cost_exceeded", "error": err_msg})
                raise PermissionError(err_msg)

            # 2. Athena Execution
            try:
                df = self.db_manager.execute_query(sql)
                trace.append({"attempt": attempt, "status": "success", "error": None})
                break
            except Exception as e:
                last_error = str(e)
                trace.append({"attempt": attempt, "status": "error", "error": last_error})

                if attempt == max_retries:
                    raise RuntimeError(f"Execution failed after {max_retries} attempts. Last Athena error: {last_error}")

                # Append error trace to context for self-healing reflection
                messages.append({"role": "assistant", "content": [{"text": sql}]})
                correction_prompt = (
                    f"Attempt {attempt} failed with the following database error:\n{last_error}\n\n"
                    "Analyze the Athena Trino error trace, inspect the column names and schemas carefully, "
                    "and provide a corrected Presto/Trino SQL query with no conversational markdown."
                )
                messages.append({"role": "user", "content": [{"text": correction_prompt}]})

        # Storage and partition advice based on final query
        index_rec = self.advisor.get_advice(final_sql, df)

        return df, final_sql, trace, examples, index_rec

    def summarize_results(self, natural_query: str, sql: str, df_preview: str) -> str:
        """Synthesizes executive insights and business summaries from execution results."""
        prompt = (
            "You are an executive business intelligence advisor.\n"
            f"User Question: {natural_query}\n"
            f"Executed Athena SQL: {sql}\n"
            f"Query Results (Preview):\n{df_preview}\n\n"
            "Provide a concise, data-driven executive summary (2-3 sentences max). "
            "Highlight key totals, top performers, percentages, or anomalies directly."
        )

        try:
            response = self.bedrock.converse(
                modelId=self.model,
                messages=[{"role": "user", "content": [{"text": prompt}]}],
                inferenceConfig={"temperature": 0.2, "maxTokens": 300}
            )
            return response["output"]["message"]["content"][0]["text"].strip()
        except Exception as e:
            return f"Analysis completed. (Automated summary unavailable: {e})"