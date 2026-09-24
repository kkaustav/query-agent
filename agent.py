import re
from typing import Tuple, List, Dict, Any, Optional
import pandas as pd
import boto3
from database import DatabaseManager, QueryCostExceededError
from example_store import ExampleStore
from index_advisor import IndexAdvisor

class NLQueryAgent:
    def __init__(
        self,
        aws_access_key: str = None,
        aws_secret_key: str = None,
        aws_region: str = "us-east-1",
        model: str = "amazon.nova-pro-v1:0",
        db_manager: DatabaseManager = None
    ):
        # Configure Bedrock client (falls back to ~/.aws/credentials if keys are not explicitly passed)
        client_kwargs = {"region_name": aws_region}
        if aws_access_key and aws_secret_key:
            client_kwargs["aws_access_key_id"] = aws_access_key
            client_kwargs["aws_secret_access_key"] = aws_secret_key

        self.bedrock = boto3.client("bedrock-runtime", **client_kwargs)
        self.model = model
        self.db = db_manager or DatabaseManager()
        self.schema = self.db.get_schema()
        self.dialect = self.db.dialect.upper()
        self.example_store = ExampleStore(bedrock_client=self.bedrock)
        self.index_advisor = IndexAdvisor(bedrock_client=self.bedrock, model=self.model, scan_threshold=2)

    def _clean_sql(self, raw_sql: str) -> str:
        sql = raw_sql.strip()
        sql = re.sub(r"^```(?:sql)?", "", sql, flags=re.IGNORECASE)
        sql = re.sub(r"```$", "", sql).strip()
        return sql.rstrip(";")

    def _format_examples_block(self, examples: List[Dict[str, Any]]) -> str:
        if not examples:
            return ""
        blocks = ["\nVERIFIED REFERENCE EXAMPLES FOR IN-CONTEXT LEARNING:"]
        for i, ex in enumerate(examples, 1):
            blocks.append(f"-- Example {i} (Match Score: {ex['score']:.2f})")
            blocks.append(f"-- Question: {ex['question']}")
            blocks.append(f"{ex['sql']}\n")
        return "\n".join(blocks)

    def _generate_initial_sql(
        self,
        natural_query: str,
        retrieved_examples: List[Dict[str, Any]],
        history: List[Dict[str, str]] = None
    ) -> Tuple[str, List[Dict[str, Any]]]:
        examples_text = self._format_examples_block(retrieved_examples)

        system_prompt = f"""You are an expert {self.dialect} database architect. Translate user inquiries into syntactically valid {self.dialect} SELECT statements based ONLY on the provided schema.

DATABASE DIALECT: {self.dialect}

SCHEMA:
{self.schema}
{examples_text}

DIALECT INSTRUCTIONS:
- If SQLITE: Use strftime('%Y-%m', col) for dates, '||' for concat.
- If POSTGRESQL: Use DATE_TRUNC('month', col) or TO_CHAR(col, 'YYYY-MM'), EXTRACT(YEAR FROM col), ILIKE for case-insensitive matching.
- If MYSQL: Use DATE_FORMAT(col, '%Y-%m'), YEAR(col), CONCAT().

RULES:
1. Return ONLY the raw SQL query. No markdown formatting (no ```sql), no conversational commentary.
2. Only write SELECT or WITH statements.
3. If the user refers to prior context (e.g. 'filter that to Europe'), adapt the previous query.
"""
        messages = []
        if history:
            for turn in history[-3:]:
                messages.append({"role": "user", "content": [{"text": turn["question"]}]})
                messages.append({"role": "assistant", "content": [{"text": turn["sql"]}]})

        messages.append({"role": "user", "content": [{"text": natural_query}]})

        response = self.bedrock.converse(
            modelId=self.model,
            system=[{"text": system_prompt}],
            messages=messages,
            inferenceConfig={"temperature": 0.0}
        )
        sql = self._clean_sql(response["output"]["message"]["content"][0]["text"])
        messages.append({"role": "assistant", "content": [{"text": sql}]})
        return sql, messages

    def _refine_sql(self, error_message: str, messages: List[Dict[str, Any]]) -> Tuple[str, List[Dict[str, Any]]]:
        prompt = f"""The previous query execution failed.

ERROR OR REJECTION REASON:
{error_message}

Review the {self.dialect} schema and syntax rules again, correct the query, and return ONLY the raw SQL."""
        messages.append({"role": "user", "content": [{"text": prompt}]})

        response = self.bedrock.converse(
            modelId=self.model,
            messages=messages,
            inferenceConfig={"temperature": 0.0}
        )
        sql = self._clean_sql(response["output"]["message"]["content"][0]["text"])
        messages.append({"role": "assistant", "content": [{"text": sql}]})
        return sql, messages

    def run_query_with_self_healing(
        self,
        natural_query: str,
        history: List[Dict[str, str]] = None,
        max_retries: int = 3
    ) -> Tuple[pd.DataFrame, str, List[Dict[str, Any]], List[Dict[str, Any]], Optional[Dict[str, Any]]]:
        retrieved_examples = self.example_store.retrieve(natural_query, top_k=2)
        sql, messages = self._generate_initial_sql(natural_query, retrieved_examples, history=history)
        trace = []

        for attempt in range(1, max_retries + 1):
            log_entry = {"attempt": attempt, "sql": sql, "error": None, "status": "pending", "cost_info": None}

            try:
                df, plan_info = self.db.execute_query(sql, check_cost=True)
                log_entry["status"] = "success"
                log_entry["cost_info"] = plan_info
                trace.append(log_entry)

                recommendation = self.index_advisor.record_and_evaluate(
                    dialect=self.db.dialect,
                    plan_info=plan_info,
                    executed_sql=sql,
                    schema=self.schema
                )

                return df, sql, trace, retrieved_examples, recommendation

            except QueryCostExceededError as ce:
                err_msg = f"PLANNER REJECTION: {str(ce)}"
                log_entry["status"] = "cost_exceeded"
                log_entry["error"] = err_msg
                trace.append(log_entry)

                if attempt == max_retries:
                    raise RuntimeError(f"Exhausted {max_retries} attempts fixing query cost: {err_msg}")
                sql, messages = self._refine_sql(f"{err_msg}. Add indexes or limit scans.", messages)

            except TimeoutError as te:
                err_msg = f"TIMEOUT: {str(te)}"
                log_entry["status"] = "timeout"
                log_entry["error"] = err_msg
                trace.append(log_entry)

                if attempt == max_retries:
                    raise RuntimeError(f"Exhausted {max_retries} attempts resolving query timeouts: {err_msg}")
                sql, messages = self._refine_sql(f"{err_msg}. Simplify joins.", messages)

            except Exception as e:
                err_msg = str(e)
                log_entry["status"] = "failed"
                log_entry["error"] = err_msg
                trace.append(log_entry)

                if attempt == max_retries:
                    raise RuntimeError(f"Exhausted {max_retries} attempts resolving database errors: {err_msg}")
                sql, messages = self._refine_sql(err_msg, messages)

        raise RuntimeError("Autonomous execution loop terminated unexpectedly.")

    def summarize_results(self, natural_query: str, sql: str, df_summary: str) -> str:
        prompt = f"""User Question: "{natural_query}"
SQL Executed: {sql}
Results Preview:
{df_summary}

Provide a concise, professional 1-2 sentence executive answer answering the user's question directly."""
        response = self.bedrock.converse(
            modelId=self.model,
            messages=[{"role": "user", "content": [{"text": prompt}]}],
            inferenceConfig={"temperature": 0.3}
        )
        return response["output"]["message"]["content"][0]["text"].strip()