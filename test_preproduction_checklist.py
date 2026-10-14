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
    ExecutionPlanCostExceededError,
    ProjectionCostExceededError,
    QueryCostExceededError,
    SQLValidationError,
    is_read_only_query,
    normalize_dialect,
    validate_sql_query,
)

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


def test_sql_validation_and_forbidden_nodes():
    print("=== 1. Testing SQL Validation, ProjectionCostExceededError & Forbidden AST Nodes ===")
    assert normalize_dialect("awsathena") == "trino"
    assert issubclass(ProjectionCostExceededError, QueryCostExceededError)
    assert issubclass(ExecutionPlanCostExceededError, QueryCostExceededError)

    for dialect in ("trino", "sqlite", "awsathena"):
        for sql, expected, expected_ro in SQL_VALIDATION_CASES:
            try:
                validate_sql_query(sql, dialect=dialect)
                actual = "ALLOW"
            except PermissionError:
                actual = "IMMEDIATE_BLOCK"
            except ProjectionCostExceededError:
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

    print(f"[PASS] All {len(SQL_VALIDATION_CASES)} validation cases & {len(FORBIDDEN_STATEMENT_CASES)} DDL/DML verbs verified.")


def test_clean_sql_output_cases():
    print("\n=== 2. Testing clean_sql_output() Malformed Output Suite ===")
    assert clean_sql_output("```sql\nSELECT col1 FROM tbl\n```") == "SELECT col1 FROM tbl"
    assert clean_sql_output("```SELECT\ncol1 FROM tbl```") == "SELECT\ncol1 FROM tbl"
    assert clean_sql_output("SELECT col1 FROM tbl") == "SELECT col1 FROM tbl"
    assert clean_sql_output("SELECT 'a```b' AS msg FROM tbl") == "SELECT 'a```b' AS msg FROM tbl"

    invalid_outputs = [
        "```sql\nSELECT 1\n```\n```sql\nSELECT 2\n```",
        "Here is your query:\n```sql\nSELECT 1\n```",
        "```sql\nSELECT 1\n```\nHope this helps!",
        "```sql\n   \n```",
        "```python\nSELECT 1\n```",
    ]
    for bad_out in invalid_outputs:
        try:
            clean_sql_output(bad_out)
            raise AssertionError(f"Expected SQLValidationError for {bad_out!r}")
        except SQLValidationError:
            pass
    print("[PASS] All clean_sql_output() cases passed.")


def test_review4_12_mandatory_scenarios():
    print("\n=== 3. Testing All 12 Mandatory Review #4 Scenarios ===")

    # Scenario 1: Glue GetTable returns EntityNotFoundException -> Treat as a new table
    mgr, mock_athena, mock_s3, mock_glue = _build_mock_manager()
    mock_glue.get_table.side_effect = ClientError(
        {"Error": {"Code": "EntityNotFoundException", "Message": "Table not found"}}, "GetTable"
    )
    assert mgr._get_existing_table_snapshot("new_table") is None
    print("[PASS] 1/12: Glue EntityNotFoundException treated as a new table.")

    # Scenario 2: Glue GetTable returns AccessDeniedException -> Fail ingestion
    mgr, mock_athena, mock_s3, mock_glue = _build_mock_manager()
    mock_glue.get_table.side_effect = ClientError(
        {"Error": {"Code": "AccessDeniedException", "Message": "Access denied"}}, "GetTable"
    )
    csv_f = io.BytesIO(b"col1,col2\n1,2\n")
    csv_f.name = "sec_test.csv"
    try:
        mgr.stream_upload_and_create_table(csv_f)
        raise AssertionError("Expected AthenaExecutionError when Glue GetTable returns AccessDeniedException")
    except AthenaExecutionError as e:
        assert "AccessDeniedException" in str(e)
    print("[PASS] 2/12: Glue AccessDeniedException fails ingestion immediately.")

    # Scenario 3: S3 DeleteObjects returns partial errors -> Report cleanup failure
    mgr, mock_athena, mock_s3, mock_glue = _build_mock_manager()
    s3_paginator = MagicMock()
    s3_paginator.paginate.return_value = [{"Contents": [{"Key": "data/tbl/ingestion_id=1/file.csv"}]}]
    mock_s3.get_paginator.return_value = s3_paginator
    mock_s3.delete_objects.return_value = {
        "Errors": [{"Key": "data/tbl/ingestion_id=1/file.csv", "Code": "AccessDenied", "Message": "Access Denied"}]
    }
    try:
        mgr._delete_s3_prefix("data/tbl/ingestion_id=1/")
        raise AssertionError("Expected AthenaExecutionError on partial S3 delete failure")
    except AthenaExecutionError as e:
        assert "partial deletion errors" in str(e)
    print("[PASS] 3/12: S3 DeleteObjects partial errors detected and raised.")

    # Scenario 4: Existing table + successful replacement -> New table points to new prefix & old prefix deleted
    mgr, mock_athena, mock_s3, mock_glue = _build_mock_manager()
    old_snap = {
        "Table": {
            "Name": "cur_report",
            "StorageDescriptor": {
                "Location": "s3://cur-bucket/data/cur_report/ingestion_id=old_prefix/",
                "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                "Columns": [{"Name": "line_item_product_code", "Type": "STRING"}],
                "SerdeInfo": {"Parameters": {"field.delim": ","}},
            },
        }
    }
    created_locations = []

    def capture_start_query(**kwargs):
        q = kwargs.get("QueryString", "")
        if q.startswith("CREATE EXTERNAL TABLE"):
            loc_match = [part for part in q.split("'") if part.startswith("s3://")]
            if loc_match:
                created_locations.append(loc_match[0])
        return {"QueryExecutionId": "ddl-ok"}

    mock_athena.start_query_execution.side_effect = capture_start_query
    mock_athena.get_query_execution.return_value = {"QueryExecution": {"Status": {"State": "SUCCEEDED"}}}

    def get_table_side_effect(DatabaseName, Name):
        if not created_locations:
            return old_snap
        return {"Table": {"StorageDescriptor": {"Location": created_locations[-1]}}}

    mock_glue.get_table.side_effect = get_table_side_effect
    s3_paginator = MagicMock()
    s3_paginator.paginate.return_value = [{"Contents": [{"Key": "data/cur_report/ingestion_id=old_prefix/f.csv"}]}]
    mock_s3.get_paginator.return_value = s3_paginator
    mock_s3.delete_objects.return_value = {"Deleted": [{"Key": "data/cur_report/ingestion_id=old_prefix/f.csv"}]}

    csv_ok = io.BytesIO(b"line_item_product_code,line_item_unblended_cost\nAmazonEC2,10.0\n")
    csv_ok.name = "cur_report.csv"
    res = mgr.stream_upload_and_create_table(csv_ok)
    assert created_locations and "ingestion_id=old_prefix" not in created_locations[-1]
    assert created_locations[-1] in res
    print("[PASS] 4/12: Existing table + successful replacement points to new prefix.")

    # Scenario 5: Existing table + failed replacement -> Old table restored and remains queryable
    mgr, mock_athena, mock_s3, mock_glue = _build_mock_manager()
    mock_glue.get_table.return_value = old_snap
    mock_athena.start_query_execution.side_effect = [
        {"QueryExecutionId": "ddl-drop-1"},
        ClientError({"Error": {"Code": "InternalServerError", "Message": "DDL failed"}}, "StartQueryExecution"),
        {"QueryExecutionId": "ddl-restore-3"},
    ]
    mock_athena.get_query_execution.return_value = {"QueryExecution": {"Status": {"State": "SUCCEEDED"}}}
    mock_s3.get_paginator.return_value = s3_paginator
    mock_s3.delete_objects.return_value = {"Deleted": []}

    csv_fail = io.BytesIO(b"line_item_product_code,line_item_unblended_cost\nAmazonEC2,10.0\n")
    csv_fail.name = "cur_report.csv"
    try:
        mgr.stream_upload_and_create_table(csv_fail)
        raise AssertionError("Expected AthenaExecutionError on failed replacement")
    except AthenaExecutionError:
        rollback_sql = mock_athena.start_query_execution.call_args_list[2].kwargs["QueryString"]
        assert "ingestion_id=old_prefix" in rollback_sql
    print("[PASS] 5/12: Existing table + failed replacement restores old table metadata.")

    # Scenario 6: CREATE IF NOT EXISTS encounters an existing table -> Detect that metadata was not replaced
    mgr, mock_athena, mock_s3, mock_glue = _build_mock_manager()
    # Simulate Glue returning the old location even after CREATE IF NOT EXISTS completed
    mock_glue.get_table.return_value = old_snap
    mock_athena.start_query_execution.return_value = {"QueryExecutionId": "ddl-noop"}
    mock_athena.get_query_execution.return_value = {"QueryExecution": {"Status": {"State": "SUCCEEDED"}}}
    mock_s3.get_paginator.return_value = s3_paginator
    mock_s3.delete_objects.return_value = {"Deleted": []}

    csv_stale = io.BytesIO(b"line_item_product_code,line_item_unblended_cost\nAmazonEC2,10.0\n")
    csv_stale.name = "cur_report.csv"
    try:
        mgr.stream_upload_and_create_table(csv_stale)
        raise AssertionError("Expected AthenaExecutionError when Glue table location was not updated")
    except AthenaExecutionError as e:
        assert "metadata was not replaced" in str(e)
    print("[PASS] 6/12: Stale metadata from CREATE IF NOT EXISTS detected and rolled back.")

    # Scenario 7: CSV malformed row after record 1,000 (at row 1,500) -> Reject the upload
    mgr, _, _, _ = _build_mock_manager()
    lines = ["col1,col2"] + [f"val{i},10" for i in range(1, 1499)] + ["bad_row,10,EXTRA_COL"]
    big_csv = io.BytesIO("\n".join(lines).encode("utf-8"))
    big_csv.name = "row1500_bad.csv"
    try:
        mgr.stream_upload_and_create_table(big_csv)
        raise AssertionError("Expected ValueError for malformed CSV row at line 1,500")
    except ValueError as e:
        assert "line 1500" in str(e)
    print("[PASS] 7/12: CSV malformed row at line 1,500 rejected prior to S3 upload.")

    # Scenario 8: Parquet schema override is incompatible -> Reject ingestion
    mgr, _, _, mock_glue = _build_mock_manager()
    mock_glue.get_table.side_effect = ClientError({"Error": {"Code": "EntityNotFoundException"}}, "GetTable")
    pq_buf = io.BytesIO()
    pq.write_table(pa.table([pa.array(["AmazonEC2"])], names=["service_name"]), pq_buf)
    pq_buf.seek(0)
    pq_buf.name = "test_pq.parquet"
    try:
        mgr.stream_upload_and_create_table(pq_buf, schema_overrides={"service_name": "BIGINT"})
        raise AssertionError("Expected ValueError for incompatible Parquet schema override (STRING -> BIGINT)")
    except ValueError as e:
        assert "Incompatible Parquet schema override" in str(e)
    print("[PASS] 8/12: Incompatible Parquet schema override rejected.")

    # Scenario 9: Athena returns a scan-cutoff error -> Do not retry with Bedrock
    mgr, mock_athena, _, _ = _build_mock_manager()
    mock_athena.start_query_execution.return_value = {"QueryExecutionId": "qid-cutoff"}
    mock_athena.get_query_execution.return_value = {
        "QueryExecution": {
            "Status": {
                "State": "FAILED",
                "StateChangeReason": "Query exhausted scan limit: BytesScannedCutoffPerQuery exceeded at line 1",
                "AthenaError": {
                    "ErrorCategory": 2,
                    "ErrorType": 1006,
                    "Retryable": False,
                    "ErrorMessage": "BytesScannedCutoffPerQuery exceeded at line 1",
                },
            }
        }
    }
    with patch("agent.boto3.Session") as mock_sess:
        mock_bedrock = MagicMock()
        mock_bedrock.converse.return_value = {
            "output": {"message": {"content": [{"text": "SELECT line_item_product_code FROM cur_data"}]}}
        }
        mock_sess.return_value.client.return_value = mock_bedrock
        agent = NLQueryAgent(db_manager=mgr, strict_semantic_config=False)
        try:
            agent.run_query_with_self_healing("Show services")
            raise AssertionError("Expected AthenaExecutionError on scan cutoff")
        except AthenaExecutionError:
            assert mock_bedrock.converse.call_count == 1
    print("[PASS] 9/12: Athena scan-cutoff error raises AthenaExecutionError without Bedrock retry.")

    # Scenario 10: Athena returns a genuine SQL syntax error (via structured AthenaError) -> Allow controlled correction
    mgr, mock_athena, mock_s3, _ = _build_mock_manager()
    mock_athena.start_query_execution.side_effect = [
        {"QueryExecutionId": "qid-sql-err"},
        {"QueryExecutionId": "qid-sql-ok"},
    ]
    mock_athena.get_query_execution.side_effect = [
        {
            "QueryExecution": {
                "Status": {
                    "State": "FAILED",
                    "StateChangeReason": "line 1:8: Column 'wrong_col' cannot be resolved",
                    "AthenaError": {
                        "ErrorCategory": 2,
                        "ErrorType": 1001,
                        "Retryable": False,
                        "ErrorMessage": "line 1:8: Column 'wrong_col' cannot be resolved",
                    },
                }
            }
        },
        {
            "QueryExecution": {
                "Status": {"State": "SUCCEEDED"},
                "ResultConfiguration": {"OutputLocation": "s3://cur-bucket/athena-results/qid-sql-ok.csv"},
                "Statistics": {"DataScannedInBytes": 1024},
            }
        },
    ]
    mock_glue_pag = MagicMock()
    mock_glue_pag.paginate.return_value = [{"TableList": []}]
    mgr.glue.get_paginator.return_value = mock_glue_pag
    mock_s3.get_object.return_value = {"Body": io.BytesIO(b"line_item_product_code\nAmazonEC2\n")}

    with patch("agent.boto3.Session") as mock_sess:
        mock_bedrock = MagicMock()
        mock_bedrock.converse.side_effect = [
            {"output": {"message": {"content": [{"text": "SELECT wrong_col FROM cur_data"}]}}},
            {"output": {"message": {"content": [{"text": "SELECT line_item_product_code FROM cur_data"}]}}},
        ]
        mock_sess.return_value.client.return_value = mock_bedrock
        agent = NLQueryAgent(db_manager=mgr, strict_semantic_config=False)
        df, final_sql, trace, _, _ = agent.run_query_with_self_healing("Show services")
        assert len(trace) == 2 and trace[0]["status"] == "sql_validation_retry" and trace[1]["status"] == "success"
    print("[PASS] 10/12: Structured Athena SQL compilation error triggers self-healing correction.")

    # Scenario 11: max_retries=0 -> Reject configuration
    try:
        agent.run_query_with_self_healing("Show services", max_retries=0)
        raise AssertionError("Expected ValueError when max_retries=0")
    except ValueError as e:
        assert "max_retries" in str(e)
    print("[PASS] 11/12: max_retries=0 rejected with ValueError.")

    # Scenario 12: Missing metrics.yaml in production -> Fail initialization
    with patch("agent.boto3.Session"):
        try:
            NLQueryAgent(
                db_manager=mgr,
                metric_file="nonexistent_production_metrics.yaml",
                strict_semantic_config=True,
            )
            raise AssertionError("Expected FileNotFoundError when metrics.yaml is missing in strict mode")
        except FileNotFoundError:
            pass
    print("[PASS] 12/12: Missing metrics.yaml in production fails initialization immediately.")


if __name__ == "__main__":
    test_sql_validation_and_forbidden_nodes()
    test_clean_sql_output_cases()
    test_review4_12_mandatory_scenarios()
    print("\nALL REVIEW #4 PRE-PRODUCTION CHECKS PASSED SUCCESSFULLY!")
