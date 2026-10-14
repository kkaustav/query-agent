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


class SQLValidationError(ValueError):
    """
    Raised when generated SQL fails syntax parsing or structural validation.
    Treated by the agent as a retryable SQL generation error.
    """
    pass


class QueryCostExceededError(SQLValidationError):
    """
    Base class for query cost guardrail violations. Subclasses SQLValidationError
    (and ValueError) so callers can distinguish retryable cost issues from syntax
    errors and non-retryable PermissionErrors.
    """
    pass


class ProjectionCostExceededError(QueryCostExceededError):
    """
    Raised when any SELECT node in the AST contains a wildcard projection
    ('SELECT *' or 'SELECT table.*').
    """
    pass


class ExecutionPlanCostExceededError(QueryCostExceededError):
    """
    Raised when a database EXPLAIN query plan exceeds configured cost or
    unindexed table-scan safety thresholds.
    """
    pass


class AthenaExecutionError(RuntimeError):
    """
    Raised when Amazon Athena or AWS infrastructure encounters a non-retryable
    runtime, permission, workgroup, scan-cutoff, or service error.
    """
    pass


class AthenaResultRetrievalError(AthenaExecutionError):
    """
    Raised when an Athena query succeeds but retrieving or parsing its result
    object from Amazon S3 fails (e.g., AccessDenied or malformed output location).
    """
    pass


class AthenaTimeoutError(TimeoutError):
    """
    Raised when an Athena query exceeds the configured execution timeout and is cancelled.
    """
    pass


FORBIDDEN_AST_NODES = tuple(
    node for node in (
        getattr(exp, "Drop", None),
        getattr(exp, "Delete", None),
        getattr(exp, "Update", None),
        getattr(exp, "Insert", None),
        getattr(exp, "Alter", None),
        getattr(exp, "AlterTable", None),
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

FORBIDDEN_LEADING_VERBS = {
    "DROP",
    "DELETE",
    "UPDATE",
    "INSERT",
    "ALTER",
    "TRUNCATE",
    "CREATE",
    "REPLACE",
    "GRANT",
    "REVOKE",
    "CALL",
    "EXEC",
    "EXECUTE",
    "SET",
    "USE",
    "MERGE",
    "MSCK",
    "SHOW",
    "DESCRIBE",
    "DESC",
    "EXPLAIN",
    "VACUUM",
    "ANALYZE",
    "COMMIT",
    "ROLLBACK",
    "BEGIN",
    "START",
    "PRAGMA",
    "COPY",
    "LOAD",
    "UNLOAD",
}

DIALECT_MAP = {
    "athena": "trino",
    "awsathena": "trino",
    "trino": "trino",
    "presto": "presto",
    "postgresql": "postgres",
    "postgres": "postgres",
    "mysql": "mysql",
    "sqlite": "sqlite",
}


def normalize_dialect(dialect: str) -> str:
    """Normalizes driver/engine dialect names (including awsathena) to SQLGlot dialects."""
    raw = (dialect or "trino").strip().lower()
    if "." in raw or "+" in raw:
        raw = raw.split("+")[0].split(".")[0]
    return DIALECT_MAP.get(raw, raw)


def validate_sql_query(sql: str, dialect: str = "trino") -> None:
    """
    Authoritative AST security, syntax, and FinOps cost validator.

    Exception Contract:
    - PermissionError (non-retryable): Empty query, multi-statement query,
      repeated trailing semicolons (e.g. 'SELECT 1;;'), or any DDL/DML/administrative
      statement (DROP, DELETE, UPDATE, CREATE, TRUNCATE, GRANT, REVOKE, CALL, SET, USE, etc.).
    - ProjectionCostExceededError (retryable subclass of QueryCostExceededError):
      Any SELECT node in the AST (top-level, CTE, or subquery) projects '*' or 'table.*'.
    - SQLValidationError (retryable subclass of ValueError): Malformed SQL syntax or
      empty SELECT projection list (e.g. 'SELECT FROM cur_data').
    """
    if not sql or not sql.strip():
        raise PermissionError("Security Guardrail: Empty SQL query is not permitted.")

    target_dialect = normalize_dialect(dialect)
    cleaned = sql.strip()

    # 1. Tokenize using sqlglot so semicolons/keywords inside string literals or comments are ignored
    try:
        tokens = sqlglot.tokenize(cleaned, read=target_dialect)
    except Exception as e:
        raise SQLValidationError(
            f"SQL Syntax Error during tokenization in '{target_dialect}' dialect: {e}"
        ) from e

    if not tokens:
        raise PermissionError("Security Guardrail: Empty or comment-only SQL query is not permitted.")

    # Reject any semicolon that is not the single final token (catches 'SELECT 1; SELECT 2' AND 'SELECT 1;;')
    if any(tok.token_type == TokenType.SEMICOLON for tok in tokens[:-1]):
        raise PermissionError(
            "Security Guardrail: Multi-statement SQL execution or repeated semicolons are prohibited."
        )

    # Remove at most one optional trailing semicolon token
    has_single_trailing_semicolon = tokens[-1].token_type == TokenType.SEMICOLON
    if has_single_trailing_semicolon:
        tokens.pop()

    if not tokens:
        raise PermissionError("Security Guardrail: Empty SQL query is not permitted.")

    # Find first non-parenthesis token to catch dialect-unparsed administrative verbs (e.g. CALL, REVOKE, TRUNCATE)
    first_word_token = next(
        (tok for tok in tokens if tok.token_type != TokenType.L_PAREN),
        tokens[0],
    )
    if first_word_token.text.upper() in FORBIDDEN_LEADING_VERBS:
        raise PermissionError(
            f"Security Guardrail: Mutating or administrative command '{first_word_token.text.upper()}' is prohibited."
        )

    # Catch 'SELECT FROM ...' missing projection syntax error at token level before AST parse
    for idx in range(len(tokens) - 1):
        if tokens[idx].token_type == TokenType.SELECT and tokens[idx + 1].token_type == TokenType.FROM:
            raise SQLValidationError(
                "SQL Syntax Error: SELECT clause is missing column projections before FROM."
            )

    # Strip only the single optional trailing semicolon from the source string
    if has_single_trailing_semicolon:
        last_semi_pos = cleaned.rfind(";")
        cleaned_for_parse = (cleaned[:last_semi_pos] + cleaned[last_semi_pos + 1 :]).strip()
    else:
        cleaned_for_parse = cleaned

    # 2. Strict single-dialect AST parse (no fallback parser)
    try:
        statements = sqlglot.parse(
            cleaned_for_parse,
            read=target_dialect,
            error_level=sqlglot.ErrorLevel.RAISE,
        )
    except sqlglot.errors.ParseError as e:
        raise SQLValidationError(f"SQL Syntax Error in '{target_dialect}' dialect: {e}") from e

    if len(statements) != 1 or statements[0] is None:
        raise PermissionError("Security Guardrail: Exactly one read-only SQL statement is permitted.")

    ast = statements[0]

    # Unwrap top-level parenthesized queries (e.g. '(SELECT col FROM tbl)')
    root_node = ast
    while isinstance(root_node, exp.Subquery) and root_node.this is not None:
        root_node = root_node.this

    # 3. AST Root & Recursive Node Check (AST is the authoritative security boundary)
    if FORBIDDEN_AST_NODES and (
        isinstance(ast, FORBIDDEN_AST_NODES)
        or isinstance(root_node, FORBIDDEN_AST_NODES)
        or any(ast.find_all(*FORBIDDEN_AST_NODES))
    ):
        raise PermissionError(
            "Security Guardrail: Mutating or administrative SQL construct detected in AST."
        )

    if not isinstance(root_node, (exp.Select, exp.SetOperation)):
        raise PermissionError(
            f"Security Guardrail: Statement root '{type(root_node).__name__}' is not a read-only SELECT query."
        )

    # 4. Recursive FinOps Wildcard & Non-Empty Projection Check across ALL SELECT nodes
    select_nodes = list(ast.find_all(exp.Select))
    if not select_nodes:
        raise PermissionError("Security Guardrail: Query does not contain a valid SELECT clause.")

    for select_node in select_nodes:
        if not select_node.expressions:
            raise SQLValidationError(
                "SQL Syntax Error: SELECT clause must specify at least one column or expression."
            )

        for projection in select_node.expressions:
            target = projection.this if isinstance(projection, exp.Alias) else projection
            # Blocks SELECT * and SELECT table.* while allowing aggregate COUNT(*)
            if isinstance(target, exp.Star) or (
                isinstance(target, exp.Column) and isinstance(target.this, exp.Star)
            ):
                raise ProjectionCostExceededError(
                    "FinOps Cost Guardrail: Wildcard projections ('SELECT *' or 'SELECT table.*') "
                    "are blocked across all queries, CTEs, and subqueries to prevent full-column scans. "
                    "Please explicitly list only the required columns."
                )


def is_read_only_query(sql: str, dialect: str = "trino") -> bool:
    """
    Returns True only if the query is syntactically valid and read-only
    (including read-only queries blocked by ProjectionCostExceededError / QueryCostExceededError).
    Returns False for PermissionError (DDL/DML/multi-statement) and ValueError (invalid syntax).
    """
    try:
        validate_sql_query(sql, dialect=dialect)
        return True
    except QueryCostExceededError:
        return True
    except (PermissionError, ValueError):
        return False


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
        if timeout_sec <= 0:
            raise ValueError("timeout_sec must be a positive integer.")
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
        self.dialect: str = normalize_dialect(self.engine.dialect.name)

        if self.dialect == "sqlite":
            self._register_sqlite_progress_handler()

    def _register_sqlite_progress_handler(self):
        """Attaches a monotonic timer to SQLite via an in-memory dictionary to support Python 3.12+."""
        timeout_limit = self.timeout_sec
        active_timers = {}

        @event.listens_for(self.engine, "connect")
        def set_sqlite_hooks(dbapi_connection, connection_record):
            conn_id = id(dbapi_connection)
            active_timers[conn_id] = 0.0

            def progress_callback():
                start_time = active_timers.get(conn_id, 0.0)
                if start_time > 0.0 and (time.monotonic() - start_time) > timeout_limit:
                    return 1  # Abort query execution
                return 0

            dbapi_connection.set_progress_handler(progress_callback, 1000)

        @event.listens_for(self.engine, "before_cursor_execute")
        def start_timer(conn, cursor, statement, parameters, context, executemany):
            raw_conn = getattr(conn.connection, "dbapi_connection", getattr(conn.connection, "connection", None))
            if raw_conn:
                active_timers[id(raw_conn)] = time.monotonic()

        @event.listens_for(self.engine, "after_cursor_execute")
        def stop_timer(conn, cursor, statement, parameters, context, executemany):
            raw_conn = getattr(conn.connection, "dbapi_connection", getattr(conn.connection, "connection", None))
            if raw_conn:
                active_timers[id(raw_conn)] = 0.0

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
        Validates query read-only status, syntax, and recursive wildcard rules via AST.
        Raises PermissionError for security violations, ProjectionCostExceededError for wildcards,
        and SQLValidationError for syntax errors.
        """
        validate_sql_query(sql, dialect=self.dialect)
        return True, ""

    def inspect_query_cost(
        self, sql: str, max_cost_pg: float = 25000.0, max_cost_mysql: float = 1000.0
    ) -> Dict[str, Any]:
        """
        Evaluates database execution plans before running queries to block expensive runaway scans.
        Raises ExecutionPlanCostExceededError when estimated plan costs exceed thresholds.
        """
        cleaned_sql = sql.strip().rstrip(";")
        with self.engine.connect() as conn:
            if self.dialect in ("postgresql", "postgres"):
                explain_stmt = text(f"EXPLAIN (FORMAT JSON) {cleaned_sql}")
                raw_result = conn.execute(explain_stmt).scalar()
                plan_data = json.loads(raw_result) if isinstance(raw_result, str) else raw_result
                plan_node = plan_data[0]["Plan"]
                total_cost = float(plan_node.get("Total Cost", 0.0))

                if total_cost > max_cost_pg:
                    raise ExecutionPlanCostExceededError(
                        f"PostgreSQL Total Cost estimated at {total_cost:.1f} (limit: {max_cost_pg:.1f})."
                    )
                return {"dialect": "postgres", "estimated_cost": total_cost, "raw_plan": plan_data}

            elif self.dialect == "mysql":
                explain_stmt = text(f"EXPLAIN FORMAT=JSON {cleaned_sql}")
                raw_result = conn.execute(explain_stmt).scalar()
                plan_data = json.loads(raw_result) if isinstance(raw_result, str) else raw_result
                cost_info = plan_data.get("query_block", {}).get("cost_info", {})
                total_cost = float(cost_info.get("query_cost", 0.0))

                if total_cost > max_cost_mysql:
                    raise ExecutionPlanCostExceededError(
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
                    raise ExecutionPlanCostExceededError(
                        f"SQLite planner detected {len(unindexed_scans)} unindexed table scans: {'; '.join(unindexed_scans)}"
                    )
                return {"dialect": "sqlite", "plan_details": details, "hazards": unindexed_scans}

        return {"status": "skipped"}

    def execute_query(self, sql: str, check_cost: bool = True) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """
        Enforces the AST security & cost boundary, inspects EXPLAIN plans,
        and executes the read-only SQL statement against the target database.
        Note: Production deployments should also use a SELECT-only database user.
        """
        validate_sql_query(sql, dialect=self.dialect)
        cleaned_sql = sql.strip()
        if cleaned_sql.endswith(";"):
            cleaned_sql = cleaned_sql[:-1].strip()

        plan_info = self.inspect_query_cost(cleaned_sql) if check_cost else {}

        try:
            with self.engine.begin() as connection:
                if self.dialect in ("postgresql", "postgres"):
                    connection.execute(text("SET TRANSACTION READ ONLY"))
                df = pd.read_sql_query(text(cleaned_sql), connection)
                return df, plan_info
        except OperationalError as e:
            error_str = str(e).lower()
            if any(term in error_str for term in ["timeout", "interrupted", "query aborted", "3024", "1317"]):
                raise TimeoutError(f"Query exceeded execution limit of {self.timeout_sec}s and was terminated.") from e
            raise RuntimeError(f"Database execution error: {str(e)}") from e
