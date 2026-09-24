import json
import re
from collections import defaultdict
from typing import Dict, List, Optional, Any

class IndexAdvisor:
    def __init__(self, bedrock_client, model: str = "amazon.nova-pro-v1:0", scan_threshold: int = 2):
        self.client = bedrock_client
        self.model = model
        self.scan_threshold = scan_threshold
        self.scan_registry: Dict[str, List[str]] = defaultdict(list)
        self.recommendations: Dict[str, Dict[str, Any]] = {}

    def extract_scanned_tables(self, dialect: str, plan_info: Dict[str, Any]) -> List[str]:
        scanned_tables = set()

        if dialect == "sqlite":
            details = plan_info.get("plan_details", [])
            for line in details:
                match = re.search(r"\bSCAN(?:\s+TABLE)?\s+([a-zA-Z0-9_]+)", line)
                if match and "USING INDEX" not in line and "USING COVERING INDEX" not in line:
                    scanned_tables.add(match.group(1))

        elif dialect == "postgresql":
            raw_plan = plan_info.get("raw_plan")
            if raw_plan and isinstance(raw_plan, list):
                def find_seq_scans(node):
                    if node.get("Node Type") == "Seq Scan" and "Relation Name" in node:
                        scanned_tables.add(node["Relation Name"])
                    for child in node.get("Plans", []):
                        find_seq_scans(child)
                find_seq_scans(raw_plan[0].get("Plan", {}))

        elif dialect == "mysql":
            raw_plan = plan_info.get("raw_plan", {})
            query_block = raw_plan.get("query_block", {})
            table_info = query_block.get("table", {})
            if table_info.get("access_type") == "ALL":
                scanned_tables.add(table_info.get("table_name"))
            for nested in query_block.get("nested_loop", []):
                t = nested.get("table", {})
                if t.get("access_type") == "ALL":
                    scanned_tables.add(t.get("table_name"))

        return list(scanned_tables)

    def record_and_evaluate(
        self, dialect: str, plan_info: Dict[str, Any], executed_sql: str, schema: str
    ) -> Optional[Dict[str, Any]]:
        tables = self.extract_scanned_tables(dialect, plan_info)
        new_recommendation = None

        for table in tables:
            self.scan_registry[table].append(executed_sql)
            if len(self.scan_registry[table]) >= self.scan_threshold and table not in self.recommendations:
                rec = self._generate_index_ddl(table, self.scan_registry[table], schema, dialect)
                self.recommendations[table] = rec
                new_recommendation = rec

        return new_recommendation

    def _generate_index_ddl(
        self, table: str, queries: List[str], schema: str, dialect: str
    ) -> Dict[str, Any]:
        queries_formatted = "\n---\n".join(queries[-3:])
        prompt = f"""You are a senior database architect optimizing a {dialect.upper()} database.

TABLE FREQUENTLY SCANNED: {table}
SCHEMA DEFINITIONS:
{schema}

QUERIES TRIGGERING REPEATED FULL-TABLE SCANS:
{queries_formatted}

Analyze the WHERE, JOIN ... ON, and ORDER BY clauses for table '{table}'.
Generate the single most effective CREATE INDEX statement to convert these unindexed scans into B-tree index lookups.

Respond STRICTLY with a valid JSON object matching this schema:
{{
  "table": "{table}",
  "index_name": "idx_{table}_<columns>",
  "ddl": "CREATE INDEX idx_{table}_<columns> ON {table}(...);",
  "reasoning": "1-2 sentence technical explanation."
}}"""

        response = self.client.converse(
            modelId=self.model,
            messages=[{"role": "user", "content": [{"text": prompt}]}],
            inferenceConfig={"temperature": 0.0}
        )
        raw_text = response["output"]["message"]["content"][0]["text"].strip()
        cleaned = re.sub(r"^```(?:json)?", "", raw_text, flags=re.IGNORECASE)
        cleaned = re.sub(r"```$", "", cleaned).strip()
        return json.loads(cleaned)