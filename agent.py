import inspect
import json
import re
import boto3
from botocore.exceptions import ClientError
from example_store import ExampleStore


class NLQueryAgent:

  def __init__(
      self,
      aws_access_key=None,
      aws_secret_key=None,
      aws_region="us-east-1",
      model="amazon.nova-pro-v1:0",
      db_manager=None,
  ):
    self.region = aws_region
    self.model_id = model
    self.db_manager = db_manager

    if aws_access_key and aws_secret_key:
      self.client = boto3.client(
          "bedrock-runtime",
          aws_access_key_id=aws_access_key,
          aws_secret_access_key=aws_secret_key,
          region_name=aws_region,
      )
    else:
      self.client = boto3.client("bedrock-runtime", region_name=aws_region)

    # Dynamically match supported parameters of ExampleStore
    try:
      sig = inspect.signature(ExampleStore.__init__)
      store_kwargs = {}
      for p in sig.parameters.values():
        if p.name in ("aws_region", "region", "region_name"):
          store_kwargs[p.name] = aws_region
        elif p.name in ("client", "bedrock_client"):
          store_kwargs[p.name] = self.client
      self.example_store = ExampleStore(**store_kwargs)
    except Exception:
      self.example_store = ExampleStore()

  def _call_nova(self, system_prompt: str, user_prompt: str) -> str:
    """Invokes Bedrock Nova Pro using the Converse API."""
    messages = [{"role": "user", "content": [{"text": user_prompt}]}]
    system = [{"text": system_prompt}]

    response = self.client.converse(
        modelId=self.model_id,
        messages=messages,
        system=system,
        inferenceConfig={"maxTokens": 2048, "temperature": 0.0, "topP": 0.9},
    )
    return response["output"]["message"]["content"][0]["text"]

  def generate_sql(
      self, natural_query: str, schema: str, history: list, examples: list
  ) -> str:
    system_prompt = f"""You are an elite data engineer specializing in Amazon Athena (Trino/Presto SQL).
Translate user questions into valid, highly performant Presto/Athena SQL.

DATABASE SCHEMA:
{schema}

CRITICAL RULES:
1. Always verify which table contains each requested column.
2. If the user asks for columns that reside in DIFFERENT tables (e.g. 'city' in zomato_customers and 'order_value' in zomato_orders), you MUST perform an explicit JOIN on the matching foreign key (e.g. ON zomato_orders.customer_id = zomato_customers.customer_id).
3. Never select a column from a table where that column does not exist.
4. Output ONLY the raw executable SQL statement enclosed in ```sql ... ``` markdown. Do not include introductory or closing prose.
5. Only write read-only SELECT statements. Do not write INSERT, UPDATE, DELETE, or DROP statements.
"""
    example_text = "\n\n".join(
        [f"Q: {ex['question']}\nSQL: {ex['sql']}" for ex in examples]
    )
    user_prompt = f"""REFERENCE PATTERNS:
{example_text}

NATURAL LANGUAGE QUESTION:
{natural_query}

ATHENA SQL:"""

    raw_response = self._call_nova(system_prompt, user_prompt)
    match = re.search(
        r"```(?:sql)?\s*(.*?)\s*```", raw_response, re.DOTALL | re.IGNORECASE
    )
    return match.group(1).strip() if match else raw_response.strip()

  def run_query_with_self_healing(
      self, natural_query: str, history: list, max_retries: int = 3
  ):
    """Executes query with dynamic schema detection and automated self-healing."""
    schema = self.db_manager.get_schema()

    try:
      examples = self.example_store.find_relevant(natural_query, k=2)
    except Exception:
      examples = []

    sql = self.generate_sql(natural_query, schema, history, examples)
    trace = []

    for attempt in range(1, max_retries + 1):
      try:
        df, plan = self.db_manager.execute_query(sql, check_cost=False)
        trace.append({
            "attempt": attempt,
            "sql": sql,
            "status": "success",
            "error": None,
        })
        return df, sql, trace, examples, None
      except Exception as e:
        err_msg = str(e)
        trace.append({
            "attempt": attempt,
            "sql": sql,
            "status": "error",
            "error": err_msg,
        })
        if attempt == max_retries:
          raise RuntimeError(
              f"Exhausted {max_retries} attempts resolving database errors:"
              f" {err_msg}"
          )

        repair_system = f"""You are an autonomous SQL repair agent for Amazon Athena.
The previous SQL execution failed. Inspect the Glue database schema and fix the query.

DATABASE SCHEMA:
{schema}

DIAGNOSTIC GUIDELINES:
- If the error states COLUMN_NOT_FOUND, search through all tables in the schema to find which table actually contains the missing column, then rewrite the query using an explicit JOIN.
- Output ONLY the corrected executable SQL inside ```sql ... ``` code blocks.
"""
        repair_user = f"""FAILED SQL:
{sql}

ATHENA ERROR:
{err_msg}

ORIGINAL QUESTION:
{natural_query}

CORRECTED ATHENA SQL:"""

        raw_fixed = self._call_nova(repair_system, repair_user)
        match = re.search(
            r"```(?:sql)?\s*(.*?)\s*```",
            raw_fixed,
            re.DOTALL | re.IGNORECASE,
        )
        sql = match.group(1).strip() if match else raw_fixed.strip()

  def summarize_results(
      self, natural_query: str, sql: str, df_preview: str
  ) -> str:
    system = (
        "You are an executive business intelligence analyst. Provide a crisp,"
        " 1-2 sentence executive answer summarizing key data findings without"
        " echoing raw SQL."
    )
    user = (
        f"Question: {natural_query}\nSQL: {sql}\nData:\n{df_preview}\nExecutive"
        " Summary:"
    )
    return self._call_nova(system, user)