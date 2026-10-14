import io
from unittest.mock import MagicMock, patch
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.exceptions import ClientError

from agent import NLQueryAgent, clean_sql_output
from athena_manager import AthenaManager
from database import (
    AthenaExecutionError,
    AthenaResultRetrievalError,
    AthenaTimeoutError,
    QueryCostExceededError,
    SQLValidationError,
    is_read_only_query,
    normalize_dialect,
    validate_sql_query,
)

# 14 SQL Validation Test Cases (covering Section 5.5, CTEs, parenthesized SELECT, and ;; rejection)
SQL_VALIDATION_CASES = [
    ("SELECT COUNT(*) FROM cur_data", "ALLOW", True),
    ("SELECT SUM(line_item_unblended_cost) FROM cur_data", "ALLOW", True),
    ("SELECT line_item_product_code FROM cur_data;", "ALLOW", True),
    ("(SELECT line_item_product_code FROM cur_data)", "ALLOW", True),
    (
        """WITH daily_cost AS (
            SELECT DATE(line_item_usage_start_date) AS usage_date,
                   SUM(line_item_unblended_cost) AS total_cost
            FROM cur_data GROUP BY 1
        ) SELECT usage_date, total_cost FROM daily_cost""",
        "ALLOW",
        True,
    ),
    ("SELECT 'DROP TABLE' AS message", "ALLOW", True),
    ("SELECT 'a;b' AS message", "ALLOW", True),
    ("SELECT * FROM cur_data", "COST_RETRY", True),
    ("SELECT c.* FROM cur_data c", "COST_RETRY", True),
    ("WITH x AS (SELECT * FROM cur_data) SELECT COUNT(*) FROM x", "COST_RETRY", True),
    ("SELECT * FROM (SELECT line_item_product_code FROM cur_data) t", "COST_RETRY", True),
    ("SELECT 1; SELECT 2", "IMMEDIATE_BLOCK", False),
    ("SELECT 1;;", "IMMEDIATE_BLOCK", False),
    ("SELECT FROM cur_data", "SYNTAX_RETRY", False),
]

# All 12 Forbidden DDL/DML/Admin statements from Section 1.3
FORBIDDEN_STATEMENT_CASES = [
    "DROP TABLE cur_data",
    "ALTER TABLE cur_data ADD COLUMN x STRING",
    "CREATE TABLE x AS SELECT 1",
    "TRUNCATE TABLE cur_data",
    "GRANT SELECT ON cur_data TO user1",
    "REVOKE SELECT ON cur_data FROM user1",
    "CALL system.sync_partition_metadata('db', 'tbl', 'FULL')",
    "SET SESSION query_max_run_time = '10m'",
    "USE my_catalog.my_db",
    "INSERT INTO cur_data VALUES (1)",
    "UPDATE cur_data SET line_item_unblended_cost = 0",
    "DELETE FROM cur_data",
]


def test_sql_validation_and_forbidden_nodes():
    print("=== 1. Testing SQL Validation & Forbidden AST Nodes (14 Cases + 12 DDL/DML Verbs) ===")
    assert normalize_dialect("awsathena") == "trino", "awsathena must normalize to trino"

    for dialect in ("trino", "sqlite", "awsathena"):
        for sql, expected, expected_ro in SQL_VALIDATION_CASES:
            try:
                validate_sql_query(sql, dialect=dialect)
                actual = "ALLOW"
            except PermissionError:
                actual = "IMMEDIATE_BLOCK"
            except QueryCostExceededError:
                actual = "COST_RETRY"
            except SQLValidationError:
                actual = "SYNTAX_RETRY"

            ro_actual = is_read_only_query(sql, dialect=dialect)
            assert actual == expected, f"[{dialect}] {sql!r}: expected {expected}, got {actual}"
            assert ro_actual == expected_ro, f"[{dialect}] is_read_only_query({sql!r}): expected {expected_ro}, got {ro_actual}"

        for forbidden_sql in FORBIDDEN_STATEMENT_CASES:
            try:
                validate_sql_query(forbidden_sql, dialect=dialect)
                raise AssertionError(f"[{dialect}] Expected PermissionError for {forbidden_sql!r}")
            except PermissionError:
                pass
            assert is_read_only_query(forbidden_sql, dialect=dialect) is False

    print(f"[PASS] All {len(SQL_VALIDATION_CASES)} validation cases & {len(FORBIDDEN_STATEMENT_CASES)} DDL/DML verbs verified across trino, sqlite, and awsathena.")


def test_clean_sql_output_cases():
    print("\n=== 2. Testing clean_sql_output() Malformed Output Suite (8 Cases) ===")
    # 1. One valid SQL code block
    assert clean_sql_output("```sql\nSELECT col1 FROM tbl\n```") == "SELECT col1 FROM tbl"
    # 2. Raw SQL without code block
    assert clean_sql_output("SELECT col1 FROM tbl") == "SELECT col1 FROM tbl"
    # 3. SQL containing backticks inside a single-quoted string literal
    assert clean_sql_output("SELECT 'a```b' AS msg FROM tbl") == "SELECT 'a```b' AS msg FROM tbl"

    # 4-8. Invalid/ambiguous cases that must raise SQLValidationError
    invalid_outputs = [
        "```sql\nSELECT 1\n```\n```sql\nSELECT 2\n```",  # Two code blocks
        "Here is your query:\n```sql\nSELECT 1\n```",    # Explanatory text before
        "```sql\nSELECT 1\n```\nHope this helps!",       # Explanatory text after
        "```sql\n   \n```",                              # Empty code block
        "```python\nSELECT 1\n```",                      # Unsupported language label
    ]
    for bad_out in invalid_outputs:
        try:
            clean_sql_output(bad_out)
            raise AssertionError(f"Expected SQLValidationError for {bad_out!r}")
        except SQLValidationError:
            pass
    print("[PASS] All 8 clean_sql_output() cases passed.")


def test_agent_retry_classification():
    print("\n=== 3. Testing Agent Retry Classification (7 Scenarios from Section 5.4) ===")

    def _make_agent(bedrock_outputs, execute_side_effect=None):
        mock_db = MagicMock()
        mock_db.dialect = "awsathena"
        mock_db.get_schema.return_value = "CREATE EXTERNAL TABLE cur_data (line_item_product_code STRING);"
        if execute_side_effect is not None:
            mock_db.execute_query.side_effect = execute_side_effect
        else:
            mock_db.execute_query.return_value = (pd.DataFrame({"line_item_product_code": ["AmazonEC2"]}), "ID: 1")

        with patch("agent.boto3.Session") as mock_sess:
            mock_bedrock = MagicMock()
            mock_bedrock.converse.side_effect = [
                {"output": {"message": {"content": [{"text": out}]}}} for out in bedrock_outputs
            ]
            mock_sess.return_value.client.return_value = mock_bedrock
            agent = NLQueryAgent(db_manager=mock_db)
            return agent, mock_db, mock_bedrock

    # 1. SELECT * -> Retries and succeeds on attempt 2
    agent, db, _ = _make_agent(["SELECT * FROM cur_data", "SELECT line_item_product_code FROM cur_data"])
    df, sql, trace, _, _ = agent.run_query_with_self_healing("Show services")
    assert len(trace) == 2 and trace[0]["status"] == "cost_guardrail_retry" and trace[1]["status"] == "success"

    # 2. SELECT table.* -> Retries and succeeds on attempt 2
    agent, db, _ = _make_agent(["SELECT c.* FROM cur_data c", "SELECT c.line_item_product_code FROM cur_data c"])
    df, sql, trace, _, _ = agent.run_query_with_self_healing("Show services")
    assert len(trace) == 2 and trace[0]["status"] == "cost_guardrail_retry" and trace[1]["status"] == "success"

    # 3. Invalid SQL syntax -> Retries and succeeds on attempt 2
    agent, db, _ = _make_agent(["SELECT FROM cur_data", "SELECT line_item_product_code FROM cur_data"])
    df, sql, trace, _, _ = agent.run_query_with_self_healing("Show services")
    assert len(trace) == 2 and trace[0]["status"] == "sql_validation_retry" and trace[1]["status"] == "success"

    # 4. DROP TABLE -> Immediately raises PermissionError on attempt 1 (no retry)
    agent, db, bedrock = _make_agent(["DROP TABLE cur_data", "SELECT line_item_product_code FROM cur_data"])
    try:
        agent.run_query_with_self_healing("Drop table")
        raise AssertionError("Expected PermissionError for DROP TABLE")
    except PermissionError:
        assert bedrock.converse.call_count == 1

    # 5. Athena AccessDenied -> Immediately raises AthenaExecutionError on attempt 1 (no retry)
    agent, db, bedrock = _make_agent(
        ["SELECT line_item_product_code FROM cur_data", "SELECT COUNT(*) FROM cur_data"],
        execute_side_effect=AthenaExecutionError("AccessDeniedException: User is not authorized"),
    )
    try:
        agent.run_query_with_self_healing("Show services")
        raise AssertionError("Expected AthenaExecutionError")
    except AthenaExecutionError:
        assert bedrock.converse.call_count == 1

    # 6. Athena timeout -> Immediately raises AthenaTimeoutError on attempt 1 (no retry)
    agent, db, bedrock = _make_agent(
        ["SELECT line_item_product_code FROM cur_data", "SELECT COUNT(*) FROM cur_data"],
        execute_side_effect=AthenaTimeoutError("Query timed out"),
    )
    try:
        agent.run_query_with_self_healing("Show services")
        raise AssertionError("Expected AthenaTimeoutError")
    except AthenaTimeoutError:
        assert bedrock.converse.call_count == 1

    # 7. Valid SELECT -> Executes in 1 attempt
    agent, db, bedrock = _make_agent(["SELECT line_item_product_code FROM cur_data"])
    df, sql, trace, _, _ = agent.run_query_with_self_healing("Show services")
    assert len(trace) == 1 and trace[0]["status"] == "success" and not df.empty

    print("[PASS] All 7 Agent retry classification scenarios verified.")


def test_athena_manager_integration_and_recovery():
    print("\n=== 4. Testing AthenaManager Workgroup, Pagination, S3 Retrieval & Safe Table Replacement ===")

    def _build_mock_manager():
        mock_athena = MagicMock()
        mock_s3 = MagicMock()
        mock_glue = MagicMock()

        mock_athena.get_work_group.return_value = {
            "WorkGroup": {
                "Configuration": {
                    "EnforceWorkGroupConfiguration": True,
                    "BytesScannedCutoffPerQuery": 104857600,
                }
            }
        }

        def client_factory(service_name, **kwargs):
            return {"athena": mock_athena, "s3": mock_s3, "glue": mock_glue}[service_name]

        with patch("athena_manager.boto3.client", side_effect=client_factory):
            mgr = AthenaManager(database="finops_db", bucket_name="cur-bucket", work_group="nl-bi-finops-workgroup")
        return mgr, mock_athena, mock_s3, mock_glue

    # 4.1 Fail-closed Workgroup test with specific message check
    with patch("athena_manager.boto3.client") as mock_boto:
        bad_athena = MagicMock()
        bad_athena.get_work_group.return_value = {
            "WorkGroup": {"Configuration": {"EnforceWorkGroupConfiguration": False}}
        }
        mock_boto.return_value = bad_athena
        try:
            AthenaManager(database="finops_db", bucket_name="cur-bucket", strict_workgroup_check=True)
            raise AssertionError("Expected AthenaExecutionError on missing scan cutoff")
        except AthenaExecutionError as e:
            assert "scan cutoff" in str(e), f"Unexpected error message: {e}"

    # 4.2 Athena query succeeds, but S3 result retrieval returns AccessDenied -> raises AthenaResultRetrievalError with Execution ID
    mgr, mock_athena, mock_s3, mock_glue = _build_mock_manager()
    mock_athena.start_query_execution.return_value = {"QueryExecutionId": "qid-access-denied-99"}
    mock_athena.get_query_execution.return_value = {
        "QueryExecution": {
            "Status": {"State": "SUCCEEDED"},
            "ResultConfiguration": {"OutputLocation": "s3://cur-bucket/athena-results/qid-access-denied-99.csv"},
        }
    }
    mock_s3.get_object.side_effect = ClientError(
        {"Error": {"Code": "AccessDenied", "Message": "Access Denied"}}, "GetObject"
    )
    try:
        mgr.execute_query("SELECT line_item_product_code FROM cur_data")
        raise AssertionError("Expected AthenaResultRetrievalError on S3 AccessDenied")
    except AthenaResultRetrievalError as e:
        assert "qid-access-denied-99" in str(e)

    # 4.3 Glue pagination across multiple pages
    mgr, mock_athena, mock_s3, mock_glue = _build_mock_manager()
    mock_paginator = MagicMock()
    mock_paginator.paginate.return_value = [
        {"TableList": [{"Name": "tbl_page_1", "StorageDescriptor": {"Columns": [{"Name": "c1", "Type": "string"}]}}]},
        {"TableList": [{"Name": "tbl_page_2", "StorageDescriptor": {"Columns": [{"Name": "c2", "Type": "bigint"}]}}]},
    ]
    mock_glue.get_paginator.return_value = mock_paginator
    schema_str = mgr.get_schema()
    assert "tbl_page_1" in schema_str and "tbl_page_2" in schema_str

    # 4.4 Existing table + failed replacement (restores previous table & cleans up failed new S3 prefix, keeps old prefix)
    mgr, mock_athena, mock_s3, mock_glue = _build_mock_manager()
    mock_glue.get_table.return_value = {
        "Table": {
            "Name": "cur_report",
            "StorageDescriptor": {
                "Location": "s3://cur-bucket/data/cur_report/ingestion_id=old123/",
                "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                "Columns": [{"Name": "line_item_product_code", "Type": "STRING"}],
                "SerdeInfo": {"Parameters": {"field.delim": ","}},
            },
        }
    }
    # 1st DDL (DROP) succeeds, 2nd DDL (new CREATE) fails, 3rd DDL (rollback CREATE) succeeds
    mock_athena.start_query_execution.side_effect = [
        {"QueryExecutionId": "ddl-drop-1"},
        ClientError({"Error": {"Code": "InternalServerError", "Message": "DDL failed"}}, "StartQueryExecution"),
        {"QueryExecutionId": "ddl-restore-3"},
    ]
    mock_athena.get_query_execution.return_value = {"QueryExecution": {"Status": {"State": "SUCCEEDED"}}}
    s3_list_paginator = MagicMock()
    s3_list_paginator.paginate.return_value = [{"Contents": [{"Key": "data/cur_report/ingestion_id=new/cur_report.csv"}]}]
    mock_s3.get_paginator.return_value = s3_list_paginator

    csv_file = io.BytesIO(b"line_item_product_code,line_item_unblended_cost\nAmazonEC2,12.50\n")
    csv_file.name = "cur_report.CSV"
    try:
        mgr.stream_upload_and_create_table(csv_file)
        raise AssertionError("Expected AthenaExecutionError when replacement CREATE DDL fails")
    except AthenaExecutionError:
        # Verify 3 DDL calls occurred: DROP, failed CREATE, and automatic rollback CREATE pointing to old123
        assert mock_athena.start_query_execution.call_count == 3
        rollback_sql = mock_athena.start_query_execution.call_args_list[2].kwargs["QueryString"]
        assert "ingestion_id=old123" in rollback_sql

    # 4.5 CSV invalid UTF-8 and inconsistent column count rejection
    bad_utf8 = io.BytesIO(b"col1,col2\n\xff\xfe,123\n")
    bad_utf8.name = "bad.csv"
    try:
        mgr.stream_upload_and_create_table(bad_utf8)
        raise AssertionError("Expected ValueError for invalid UTF-8")
    except ValueError as e:
        assert "UTF-8" in str(e)

    bad_cols = io.BytesIO(b"col1,col2\nval1,val2,val3\n")
    bad_cols.name = "mismatch.csv"
    try:
        mgr.stream_upload_and_create_table(bad_cols)
        raise AssertionError("Expected ValueError for inconsistent CSV column count")
    except ValueError as e:
        assert "column count" in str(e)

    # 4.6 Parquet file with duplicate/dirty column names
    mgr, mock_athena, mock_s3, mock_glue = _build_mock_manager()
    mock_glue.get_table.side_effect = ClientError({"Error": {"Code": "EntityNotFoundException"}}, "GetTable")
    mock_athena.start_query_execution.return_value = {"QueryExecutionId": "ddl-pq-1"}
    mock_athena.get_query_execution.return_value = {"QueryExecution": {"Status": {"State": "SUCCEEDED"}}}

    pq_buf = io.BytesIO()
    table = pa.table(
        [pa.array(["AmazonEC2", "AmazonS3"]), pa.array([10.5, 20.0])],
        names=["Service Name", "Service-Name"],
    )
    pq.write_table(table, pq_buf)
    pq_buf.seek(0)
    pq_buf.name = "dirty_cols.parquet"
    res_msg = mgr.stream_upload_and_create_table(pq_buf)
    assert "dirty_cols" in res_msg

    print("[PASS] All AthenaManager integration, failure-recovery, pagination, and ingestion tests passed!")


if __name__ == "__main__":
    test_sql_validation_and_forbidden_nodes()
    test_clean_sql_output_cases()
    test_agent_retry_classification()
    test_athena_manager_integration_and_recovery()
    print("\nALL PRE-PRODUCTION CHECKS PASSED SUCCESSFULLY!")
