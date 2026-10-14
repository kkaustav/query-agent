import json
import re
import time
from typing import Any, Dict, Tuple
import pandas as pd
import sqlglot
from sqlglot import exp
from sqlalchemy import create_engine, event, inspect, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError


class QueryCostExceededError(Exception):
    """Raised when a query plan or AST cost guardrail exceeds execution safety thresholds."""
    pass


FORBIDDEN_KEYWORDS = [
    r"\bDROP\b", r"\bDELETE\b", r"\bUPDATE\b", r"\bINSERT\b",
    r"\bALTER\b", r"\bTRUNCATE\b", r"\bREPLACE\b", r"\bGRANT\b",
    r"\bREVOKE\b", r"\bEXEC\b", r"\bCALL\b", r"\bCREATE\b"
]

FORBIDDEN_AST_NODES = tuple(
    node for node in (
        getattr(exp, "Drop", None),
        getattr(exp, "Delete", None),
        getattr(exp, "Update", None),
        getattr(exp, "Insert", None),
        getattr(exp, "Alter", None),
        getattr(exp, "Create", None),
        getattr(exp, "Command", None),
        getattr(exp, "Grant", None),
        getattr(exp, "Revoke", None),
        getattr(exp, "TruncateTable", None),
        getattr(exp, "Merge", None),
    )
    if node is not None
)


def is_read_only_query(sql: str, dialect: str = "trino") -> bool:
    """
    Deterministic sqlglot AST sanitizer that guarantees only a single,
    read-only SELECT statement (including WITH CTEs) is permitted to execute.
    """
    if not sql or not sql.strip():
        return False

    cleaned = sql.strip().rstrip(";")

    if ";" in cleaned:
        return False

    no_comments = re.sub(r"--.*?\n", "", cleaned, flags=re.MULTILINE)
    no_comments = re.sub(r"/\*.*?\*/", "", no_comments, flags=re.DOTALL).strip()
    for pattern in FORBIDDEN_KEYWORDS:
        if re.search(pattern, no_comments, re.IGNORECASE):
            return False

    try:
        statements = sqlglot.parse(cleaned, read=dialect)
    except Exception:
        try:
            statements = sqlglot.parse(cleaned)
        except Exception:
            return False

    if len(statements) != 1 or statements[0] is None:
        return False

    ast = statements[0]

    if not isinstance(ast, (exp.Select, exp.SetOperation)):
        return False

    if FORBIDDEN_AST_NODES and any(ast.find_all(*FORBIDDEN_AST_NODES)):
        return False

    return True


def validate_no_select_star(sql: str, dialect: str = "trino") -> Tuple[bool, str]:
    """
    FinOps AST Cost Guardrail: Recursively inspects EVERY SELECT node in the AST
    (including outer queries, CTEs, and nested subqueries) to block 'SELECT *'
    or 'SELECT table.*' full-column scans, while preserving legitimate expressions
    like 'COUNT(*)' and 'EXISTS (SELECT 1 / *)'.
    """
    cleaned = sql.strip().rstrip(";")
    try:
        statements = sqlglot.parse(cleaned, read=dialect)
    except Exception:
        try:
            statements = sqlglot.parse(cleaned)
        except Exception as e:
            return False, f"SQL Syntax Error during AST cost inspection: {e}"

    if len(statements) != 1 or statements[0] is None:
        return False, "Only a single valid SQL statement is permitted."

    ast = statements[0]

    # Traverse every SELECT node across the entire AST (outer query, CTEs, subqueries)
    for select_node in ast.find_all(exp.Select):
        # Allow SELECT * inside EXISTS (...) subqueries where columns are not materialized
        if isinstance(select_node.parent, exp.Exists):
            continue

        for projection in select_node.expressions:
            # Note: COUNT(*) is represented as exp.Count(this=exp.Star()), so projection
            # is exp.Count (or exp.Alias wrapping exp.Count), NOT exp.Star or exp.Column.
            if isinstance(projection, exp.Star) or (
                isinstance(projection, exp.Column) and isinstance(projection.this, exp.Star)
            ):
                return (
                    False,
                    "FinOps Cost Guardrail: 'SELECT *' (including inside CTEs or subqueries) is blocked "
                    "to prevent full-column scans in Amazon Athena. Please explicitly project only the required columns."
                )

    return True, ""


class DatabaseManager:
    def __init__(self, connection_uri: str = "sqlite:///data/ecommerce.db", timeout_sec: int = 5):
        self.connection_uri = connection_uri
        self.timeout_sec = timeout_sec
        self.timeout_ms = timeout_sec * 1000

        connect_args = {}
        if connection_uri.startswith("postgresql"):
            connect_args = {"options": f"-c statement_timeout={self.timeout_ms}"}
        elif connection_uri.startswith("mysql"):
            connect_args = {
                "init_command": f"SET SESSION max_execution_time={self.timeout_ms}",
                "read_timeout": self.timeout_sec
            }
        elif connection_uri.startswith("sqlite"):
            connect_args = {"check_same_thread": False, "timeout": self.timeout_sec}

        self.engine: Engine = create_engine(
            self.connection_uri,
            connect_args=connect_args,
            pool_pre_ping=True
        )
        self.dialect: str = self.engine.dialect.name

        if self.dialect == "sqlite":
            self._register_sqlite_progress_handler()

    def _register_sqlite_progress_handler(self):
        """Attaches a timer to SQLite via an in-memory dictionary to support Python 3.12+."""
        timeout_limit = self.timeout_sec
        active_timers = {}

        @event.listens_for(self.engine, "connect")
        def set_sqlite_hooks(dbapi_connection, connection_record):
            conn_id = id(dbapi_connection)
            active_timers[conn_id] = 0

            def progress_callback():
                start_time = active_timers.get(conn_id, 0)
                if start_time > 0 and (time.time() - start_time) > timeout_limit:
                    return 1  # Abort query execution
                return 0

            dbapi_connection.set_progress_handler(progress_callback, 1000)

        @event.listens_for(self.engine, "before_cursor_execute")
        def start_timer(conn, cursor, statement, parameters, context, executemany):
            raw_conn = getattr(conn.connection, "dbapi_connection", getattr(conn.connection, "connection", None))
            if raw_conn:
                active_timers[id(raw_conn)] = time.time()

        @event.listens_for(self.engine, "after_cursor_execute")
        def stop_timer(conn, cursor, statement, parameters, context, executemany):
            raw_conn = getattr(conn.connection, "dbapi_connection", getattr(conn.connection, "connection", None))
            if raw_conn:
                active_timers[id(raw_conn)] = 0

    def get_schema(self) -> str:
        """Inspects and returns DDL representation of database tables, columns, and foreign keys."""
        inspector = inspect(self.engine)
        table_names = inspector.get_table_names()
        schema_definitions = []

        for table in table_names:
            columns = inspector.get_columns(table)
            pk = inspector.get_pk_constraint(table)
            fks = inspector.get_foreign_keys(table)
            pk_cols = set(pk.get("constrained_columns", []))

            col_lines = []
            for col in columns:
                col_name = col["name"]
                col_type = str(col["type"])
                flags = []
                if col_name in pk_cols:
                    flags.append("PRIMARY KEY")
                if not col.get("nullable", True):
                    flags.append("NOT NULL")
                flag_str = f" {' '.join(flags)}" if flags else ""
                col_lines.append(f"  {col_name} {col_type}{flag_str}")

            for fk in fks:
                for c, r in zip(fk["constrained_columns"], fk["referred_columns"]):
                    col_lines.append(f"  FOREIGN KEY ({c}) REFERENCES {fk['referred_table']}({r})")

            table_def = f"CREATE TABLE {table} (\n" + ",\n".join(col_lines) + "\n);"
            schema_definitions.append(table_def)

        return "\n\n".join(schema_definitions)

    def validate_query(self, sql: str) -> Tuple[bool, str]:
        """Validates query read-only status and recursive wildcard rules via AST."""
        cleaned = sql.strip().rstrip(";")
        if ";" in cleaned:
            return False, "Multi-statement execution is disallowed for security."
        for pattern in FORBIDDEN_KEYWORDS:
            if re.search(pattern, cleaned, re.IGNORECASE):
                return False, f"Potentially destructive DDL/DML keyword detected: {pattern}"
        if not re.match(r"^\s*(SELECT|WITH)\b", cleaned, re.IGNORECASE):
            return False, "Only read-only SELECT or WITH statements are permitted."
        target_dialect = self.dialect if self.dialect != "sqlite" else "sqlite"
        if not is_read_only_query(cleaned, dialect=target_dialect):
            return False, "AST validation rejected non-SELECT or mutating SQL statement."
        is_cost_safe, cost_err = validate_no_select_star(cleaned, dialect=target_dialect)
        if not is_cost_safe:
            return False, cost_err
        return True, ""

    def inspect_query_cost(
        self, sql: str, max_cost_pg: float = 25000.0, max_cost_mysql: float = 1000.0
    ) -> Dict[str, Any]:
        """Evaluates database execution plans before running queries to block expensive runaway scans."""
        cleaned_sql = sql.strip().rstrip(";")
        with self.engine.connect() as conn:
            if self.dialect == "postgresql":
                explain_stmt = text(f"EXPLAIN (FORMAT JSON) {cleaned_sql}")
                raw_result = conn.execute(explain_stmt).scalar()
                plan_data = json.loads(raw_result) if isinstance(raw_result, str) else raw_result
                plan_node = plan_data[0]["Plan"]
                total_cost = float(plan_node.get("Total Cost", 0.0))

                if total_cost > max_cost_pg:
                    raise QueryCostExceededError(
                        f"PostgreSQL Total Cost estimated at {total_cost:.1f} (limit: {max_cost_pg:.1f})."
                    )
                return {"dialect": "postgresql", "estimated_cost": total_cost, "raw_plan": plan_data}

            elif self.dialect == "mysql":
                explain_stmt = text(f"EXPLAIN FORMAT=JSON {cleaned_sql}")
                raw_result = conn.execute(explain_stmt).scalar()
                plan_data = json.loads(raw_result) if isinstance(raw_result, str) else raw_result
                cost_info = plan_data.get("query_block", {}).get("cost_info", {})
                total_cost = float(cost_info.get("query_cost", 0.0))

                if total_cost > max_cost_mysql:
                    raise QueryCostExceededError(
                        f"MySQL query_cost estimated at {total_cost:.1f} (limit: {max_cost_mysql:.1f})."
                    )
                return {"dialect": "mysql", "estimated_cost": total_cost, "raw_plan": plan_data}

            elif self.dialect == "sqlite":
                explain_stmt = text(f"EXPLAIN QUERY PLAN {cleaned_sql}")
                rows = conn.execute(explain_stmt).fetchall()
                details = [row[-1] for row in rows]

                unindexed_scans = [
                    d for d in details
                    if d.startswith("SCAN") and "USING INDEX" not in d and "USING COVERING INDEX" not in d
                ]

                if len(unindexed_scans) >= 3:
                    raise QueryCostExceededError(
                        f"SQLite planner detected {len(unindexed_scans)} unindexed table scans: {'; '.join(unindexed_scans)}"
                    )
                return {"dialect": "sqlite", "plan_details": details, "hazards": unindexed_scans}

        return {"status": "skipped"}

    def execute_query(self, sql: str, check_cost: bool = True) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """Validates, inspects cost, and executes the SQL statement against the target database."""
        is_valid, err = self.validate_query(sql)
        if not is_valid:
            raise ValueError(err)

        plan_info = self.inspect_query_cost(sql) if check_cost else {}

        try:
            with self.engine.connect() as connection:
                if self.dialect == "postgresql":
                    connection.execute(text("SET TRANSACTION READ ONLY;"))
                df = pd.read_sql_query(text(sql), connection)
                return df, plan_info
        except OperationalError as e:
            error_str = str(e).lower()
            if any(term in error_str for term in ["timeout", "interrupted", "query aborted", "3024", "1317"]):
                raise TimeoutError(f"Query exceeded execution limit of {self.timeout_sec}s and was terminated.") from e
            raise RuntimeError(f"Database execution error: {str(e)}") from e
