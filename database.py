import json
import time
from typing import Any, Dict, Tuple
import pandas as pd
import sqlglot
from sqlglot import exp
from sqlglot.tokens import TokenType
from sqlalchemy import create_engine, event, inspect, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError


class QueryCostExceededError(ValueError):
    """
    Raised when an AST cost guardrail (such as SELECT *) or EXPLAIN plan
    exceeds execution safety thresholds. Subclasses ValueError so it is
    treated as a retryable cost/validation error rather than a PermissionError.
    """
    pass


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
        getattr(exp, "Transaction", None),
        getattr(exp, "Commit", None),
        getattr(exp, "Rollback", None),
        getattr(exp, "Set", None),
        getattr(exp, "Use", None),
        getattr(exp, "Describe", None),
        getattr(exp, "Show", None),
        getattr(exp, "LoadData", None),
        getattr(exp, "Pragma", None),
    )
    if node is not None
)

DIALECT_MAP = {
    "athena": "trino",
    "trino": "trino",
    "presto": "presto",
    "postgresql": "postgres",
    "postgres": "postgres",
    "mysql": "mysql",
    "sqlite": "sqlite",
}


def _normalize_dialect(dialect: str) -> str:
    return DIALECT_MAP.get((dialect or "trino").lower(), dialect.lower())


def validate_sql_query(sql: str, dialect: str = "trino") -> None:
    """
    Single authoritative AST security & FinOps cost boundary.

    1. Raises PermissionError (non-retryable) if:
       - The query is empty, contains multiple statements (e.g. 'SELECT 1; DROP TABLE x'),
         or is not a read-only SELECT / UNION / WITH statement.
       - Any DDL/DML/administrative node (DROP, DELETE, UPDATE, INSERT, etc.) exists in the AST.

    2. Raises QueryCostExceededError (a subclass of ValueError, retryable) if:
       - Any SELECT node anywhere in the AST (top-level, CTE, subquery, IN, EXISTS)
         projects a bare '*' or 'table.*' wildcard (COUNT(*) remains allowed).
    """
    if not sql or not sql.strip():
        raise PermissionError("Security Guardrail: Empty SQL query is not permitted.")

    target_dialect = _normalize_dialect(dialect)
    cleaned = sql.strip()

    # 1. Tokenize using sqlglot so semicolons/keywords inside string literals or comments are ignored
    try:
        tokens = sqlglot.tokenize(cleaned, read=target_dialect)
    except Exception as e:
        raise PermissionError(f"Security Guardrail: Failed to tokenize SQL in '{target_dialect}' dialect: {e}") from e

    # Strip trailing semicolons at the token level
    while tokens and tokens[-1].token_type == TokenType.SEMICOLON:
        tokens.pop()

    if not tokens:
        raise PermissionError("Security Guardrail: Empty or comment-only SQL query is not permitted.")

    # Reject multi-statement queries (any remaining SEMICOLON token outside strings/comments)
    if any(tok.token_type == TokenType.SEMICOLON for tok in tokens):
        raise PermissionError("Security Guardrail: Multi-statement SQL execution is prohibited.")

    # Check the first token to immediately classify obvious non-SELECT/WITH commands as PermissionError
    first_token_type = tokens[0].token_type
    allowed_leading_tokens = {
        TokenType.SELECT,
        TokenType.WITH,
        TokenType.L_PAREN,
    }
    if first_token_type not in allowed_leading_tokens:
        raise PermissionError(
            f"Security Guardrail: Only read-only SELECT or WITH queries are permitted "
            f"(found leading token '{tokens[0].text.upper()}')."
        )

    # 2. Strict single-dialect AST parse (no fallback parser)
    cleaned_no_trailing_semi = cleaned.rstrip("; \t\r\n")
    try:
        statements = sqlglot.parse(
            cleaned_no_trailing_semi,
            read=target_dialect,
            error_level=sqlglot.ErrorLevel.RAISE,
        )
    except sqlglot.errors.ParseError as e:
        # Leading token was already verified as SELECT/WITH; raise retryable ValueError for syntax repair
        raise ValueError(f"SQL Syntax Error in '{target_dialect}' dialect: {e}") from e

    if len(statements) != 1 or statements[0] is None:
        raise PermissionError("Security Guardrail: Exactly one read-only SQL statement is permitted.")

    ast = statements[0]

    # 3. Verify root node is a read-only query expression and contains no forbidden AST nodes anywhere
    if not isinstance(ast, (exp.Select, exp.SetOperation)):
        raise PermissionError(
            f"Security Guardrail: Statement root '{type(ast).__name__}' is not a read-only SELECT query."
        )

    if FORBIDDEN_AST_NODES and any(ast.find_all(*FORBIDDEN_AST_NODES)):
        raise PermissionError(
            "Security Guardrail: Mutating or administrative SQL construct detected in AST."
        )

    # 4. Recursive FinOps Wildcard Check across ALL SELECT nodes (outer, CTEs, subqueries, EXISTS)
    select_nodes = list(ast.find_all(exp.Select))
    if not select_nodes:
        raise PermissionError("Security Guardrail: Query does not contain a valid SELECT clause.")

    for select_node in select_nodes:
        for projection in select_node.expressions:
            target = projection.this if isinstance(projection, exp.Alias) else projection
            # Blocks SELECT * and SELECT table.* while allowing aggregate COUNT(*)
            if isinstance(target, exp.Star) or (
                isinstance(target, exp.Column) and isinstance(target.this, exp.Star)
            ):
                raise QueryCostExceededError(
                    "FinOps Cost Guardrail: Wildcard projections ('SELECT *' or 'SELECT table.*') "
                    "are blocked across all queries, CTEs, and subqueries to prevent full-column scans. "
                    "Please explicitly list only the required columns."
                )


def is_read_only_query(sql: str, dialect: str = "trino") -> bool:
    """
    Returns True if the query passes strict AST read-only security validation
    (ignoring retryable SELECT * cost errors), or False if it raises PermissionError.
    """
    try:
        validate_sql_query(sql, dialect=dialect)
        return True
    except PermissionError:
        return False
    except ValueError:
        # Query is read-only, even if it has a retryable SELECT * or syntax issue
        return True


def validate_no_select_star(sql: str, dialect: str = "trino") -> Tuple[bool, str]:
    """
    Validates that no SELECT node in the query contains a wildcard projection.
    Raises PermissionError if the query is destructive/multi-statement.
    """
    try:
        validate_sql_query(sql, dialect=dialect)
        return True, ""
    except PermissionError:
        raise
    except ValueError as e:
        return False, str(e)


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
        """
        Validates query read-only status and recursive wildcard rules via AST.
        Raises PermissionError for security violations and ValueError for wildcard/syntax violations.
        """
        validate_sql_query(sql, dialect=self.dialect)
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
        """
        Enforces the AST security & cost boundary, inspects EXPLAIN plans,
        and executes the read-only SQL statement against the target database.
        Raises PermissionError on non-SELECT/destructive SQL and ValueError on SELECT *.
        """
        validate_sql_query(sql, dialect=self.dialect)
        cleaned_sql = sql.strip().rstrip(";")

        plan_info = self.inspect_query_cost(cleaned_sql) if check_cost else {}

        try:
            with self.engine.connect() as connection:
                if self.dialect == "postgresql":
                    connection.execute(text("SET TRANSACTION READ ONLY;"))
                df = pd.read_sql_query(text(cleaned_sql), connection)
                return df, plan_info
        except OperationalError as e:
            error_str = str(e).lower()
            if any(term in error_str for term in ["timeout", "interrupted", "query aborted", "3024", "1317"]):
                raise TimeoutError(f"Query exceeded execution limit of {self.timeout_sec}s and was terminated.") from e
            raise RuntimeError(f"Database execution error: {str(e)}") from e
