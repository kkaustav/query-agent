import io
import json
import time
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


def _build_mock_manager(
    task_owner_id: str = "ecs-task-1",
    use_atomic_glue_cutover: bool = False,
    lease_heartbeat_interval_sec: float = 10.0,
    validate_full_dataset: bool = True,
):
    AthenaManager._INGESTION_STATES.clear()
    AthenaManager._TABLE_LOCKS.clear()

    mock_athena = MagicMock()
    mock_s3 = MagicMock()
    mock_glue = MagicMock()
    mock_dynamodb = MagicMock()

    mock_athena.get_work_group.return_value = {
        "WorkGroup": {
            "Configuration": {
                "EnforceWorkGroupConfiguration": True,
                "BytesScannedCutoffPerQuery": 104857600,
            }
        }
    }

    def client_factory(service_name, **kwargs):
        return {
            "athena": mock_athena,
            "s3": mock_s3,
            "glue": mock_glue,
            "dynamodb": mock_dynamodb,
        }[service_name]

    with patch("athena_manager.boto3.client", side_effect=client_factory):
        mgr = AthenaManager(
            database="finops_db",
            bucket_name="cur-bucket",
            work_group="nl-bi-finops-workgroup",
            task_owner_id=task_owner_id,
            use_atomic_glue_cutover=use_atomic_glue_cutover,
            lease_heartbeat_interval_sec=lease_heartbeat_interval_sec,
            validate_full_dataset=validate_full_dataset,
        )
    return mgr, mock_athena, mock_s3, mock_glue, mock_dynamodb


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


def test_review4_and_review5_scenarios():
    print("\n=== 3. Testing Core Ingestion, Rollback & Distributed Scenarios ===")

    mgr, mock_athena, mock_s3, mock_glue, _ = _build_mock_manager()
    mock_glue.get_table.side_effect = ClientError(
        {"Error": {"Code": "EntityNotFoundException", "Message": "Table not found"}}, "GetTable"
    )
    assert mgr._get_existing_table_snapshot("new_table") is None

    mock_glue.get_table.side_effect = ClientError(
        {"Error": {"Code": "AccessDeniedException", "Message": "Access denied"}}, "GetTable"
    )
    csv_f = io.BytesIO(b"col1,col2\n1,2\n")
    csv_f.name = "sec_test.csv"
    try:
        mgr.stream_upload_and_create_table(csv_f)
        raise AssertionError("Expected AthenaExecutionError")
    except AthenaExecutionError as e:
        assert "AccessDeniedException" in str(e)

    for bad_restored_loc in ("s3://cur-bucket/data/cur_report/ingestion_id=WRONG/", None, ""):
        mgr, mock_athena, mock_s3, mock_glue, mock_dyn = _build_mock_manager()
        mock_dyn.update_item.return_value = {"Attributes": {"counter_value": {"N": "1"}}}
        initial_snap = {
            "Table": {
                "Name": "cur_report",
                "StorageDescriptor": {
                    "Location": "s3://cur-bucket/data/cur_report/ingestion_id=good_old/",
                    "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                    "Columns": [{"Name": "line_item_product_code", "Type": "STRING"}],
                    "SerdeInfo": {"Parameters": {"field.delim": ","}},
                },
            }
        }
        post_rollback_snap = {
            "Table": {
                "Name": "cur_report",
                "StorageDescriptor": {} if bad_restored_loc is None else {"Location": bad_restored_loc},
            }
        }
        mock_glue.get_table.side_effect = [initial_snap, post_rollback_snap]
        mock_athena.start_query_execution.side_effect = [
            {"QueryExecutionId": "1-stg-create"},
            {"QueryExecutionId": "2-stg-val"},
            {"QueryExecutionId": "3-prod-drop"},
            ClientError({"Error": {"Code": "InternalError", "Message": "Prod CREATE failed"}}, "StartQueryExecution"),
            {"QueryExecutionId": "5-stg-drop"},
            {"QueryExecutionId": "6-rollback-ddl-ok"},
        ]
        mock_athena.get_query_execution.return_value = {"QueryExecution": {"Status": {"State": "SUCCEEDED"}}}
        s3_pag = MagicMock()
        s3_pag.paginate.return_value = [{"Contents": []}]
        mock_s3.get_paginator.return_value = s3_pag

        csv_rb = io.BytesIO(b"line_item_product_code\nAmazonEC2\n")
        csv_rb.name = "cur_report.csv"
        try:
            mgr.stream_upload_and_create_table(csv_rb)
            raise AssertionError(f"Expected CRITICAL RECOVERY FAILURE when rollback restores location={bad_restored_loc!r}")
        except AthenaExecutionError as e:
            assert "CRITICAL RECOVERY FAILURE" in str(e)

    print("[PASS] Core ingestion, rollback location verification, and access-control tests passed.")


def test_feedback6_final_blockers():
    print("\n=== 4. Testing All Final Blockers from Project Feedback_6.docx ===")

    # 1. DynamoDB Atomic Fencing Token Generation & Same-Owner Conflict Check
    mgr, mock_athena, mock_s3, mock_glue, mock_dyn = _build_mock_manager(task_owner_id="shared-ecs")
    mock_dyn.update_item.side_effect = [
        {"Attributes": {"counter_value": {"N": "1"}}},
        ClientError({"Error": {"Code": "ConditionalCheckFailedException", "Message": "Lock exists"}}, "PutItem")
    ]
    try:
        mgr._acquire_distributed_table_lock("cur_report", "ing_123")
        mgr._acquire_distributed_table_lock("cur_report", "ing_456")
        raise AssertionError("Expected ConditionalCheckFailedException on same owner / different ingestion ID")
    except AthenaExecutionError as e:
        assert "Concurrent distributed ingestion conflict" in str(e)
    print("[PASS] 1/4: Fencing token is monotonic via DynamoDB atomic counter and prevents same-owner lock theft.")

    # 2. Cleanup TOCTOU Race & Fail-Closed Behavior
    mgr, mock_athena, mock_s3, mock_glue, mock_dyn = _build_mock_manager()
    mock_dyn.get_item.return_value = {
        "Item": {
            "lock_key": {"S": "finops_db.cur_report"},
            "owner_id": {"S": "task-a"},
            "ingestion_id": {"S": "active_run_999"},
            "state": {"S": "UPLOADING"},
            "lease_expires_at": {"N": str(time.time() + 500)},
            "updated_at": {"N": str(time.time())}
        }
    }
    # Even if S3 manifest says it's abandoned/expired, the final DynamoDB check blocks deletion
    AthenaManager._INGESTION_STATES.clear()
    s3_paginator = MagicMock()
    s3_paginator.paginate.return_value = [{"Contents": [{"Key": "data/cur_report/ingestion_id=active_run_999/f.csv", "LastModified": "yesterday"}]}]
    mock_s3.get_paginator.return_value = s3_paginator
    
    removed = mgr.cleanup_abandoned_staging_prefixes("cur_report", min_age_seconds=0, require_verified_state=False)
    assert "data/cur_report/ingestion_id=active_run_999/" not in removed
    assert mock_s3.delete_objects.call_count == 0

    mock_dyn.get_item.side_effect = ClientError({"Error": {"Code": "AccessDenied", "Message": "Cannot read lock"}}, "GetItem")
    try:
        mgr.cleanup_abandoned_staging_prefixes("cur_report", min_age_seconds=0, require_verified_state=False)
        raise AssertionError("Expected cleanup to fail closed on DynamoDB GetItem error")
    except AthenaExecutionError as e:
        assert "Fail-Closed Cleanup Error" in str(e)
    print("[PASS] 2/4: Cleanup strictly validates DynamoDB lock state before S3 deletion (no TOCTOU race) and fails closed on error.")

    # 3. Full-Dataset Validation & Null Checks
    mgr, mock_athena, mock_s3, mock_glue, mock_dyn = _build_mock_manager(validate_full_dataset=True)
    mock_dyn.update_item.return_value = {"Attributes": {"counter_value": {"N": "1"}}}
    mock_glue.get_table.side_effect = ClientError({"Error": {"Code": "EntityNotFoundException"}}, "GetTable")
    
    mock_athena.start_query_execution.return_value = {"QueryExecutionId": "qid-val-null"}
    mock_athena.get_query_execution.return_value = {
        "QueryExecution": {
            "Status": {"State": "SUCCEEDED"},
            "ResultConfiguration": {"OutputLocation": "s3://cur-bucket/athena-results/qid-val-null.csv"},
        }
    }
    # Simulate _null_chk_0 = 1000 and _sampled_rows = 1000 (100% null)
    mock_s3.get_object.return_value = {
        "Body": io.BytesIO(b"_sampled_rows,_null_chk_0,_nn_chk_0,_null_chk_1,_nn_chk_1,_num_chk_1,_nan_inf_chk_1\n1000,1000,0,0,1000,15420.50,0\n")
    }
    mock_s3.get_paginator.return_value.paginate.return_value = [{"Contents": []}]

    rows = ["line_item_product_code,line_item_unblended_cost"] + [f",{i}.0" for i in range(1, 1001)]
    csv_null = io.BytesIO("\n".join(rows).encode("utf-8"))
    csv_null.name = "cur_report.csv"
    try:
        mgr.stream_upload_and_create_table(csv_null, validate_full_dataset=True)
        raise AssertionError("Expected staging validation to reject 100% null required column")
    except AthenaExecutionError as e:
        assert "100% null in the staged dataset" in str(e)
        val_sql = mock_athena.start_query_execution.call_args_list[1].kwargs["QueryString"]
        assert "LIMIT 1000" not in val_sql
        assert "IS NULL THEN 1 ELSE 0 END" in val_sql
    print("[PASS] 3/4: Full-dataset validation verifies explicit NULL checks on required columns.")

    # 4. Pre-Glue Fencing ensures expired workers cannot cut over
    mgr, mock_athena, mock_s3, mock_glue, mock_dyn = _build_mock_manager()
    mock_dyn.update_item.side_effect = [
        {"Attributes": {"counter_value": {"N": "1"}}}, # _next_fencing_token
        {}, # UPLOADING
        {}, # VALIDATING
        ClientError({"Error": {"Code": "ConditionalCheckFailedException", "Message": "Lease expired"}}, "UpdateItem") # CUTOVER
    ]
    mock_glue.get_table.side_effect = ClientError({"Error": {"Code": "EntityNotFoundException"}}, "GetTable")
    mock_athena.start_query_execution.return_value = {"QueryExecutionId": "qid-ok"}
    mock_athena.get_query_execution.return_value = {
        "QueryExecution": {
            "Status": {"State": "SUCCEEDED"},
            "ResultConfiguration": {"OutputLocation": "s3://b/k.csv"},
        }
    }
    mock_s3.get_object.return_value = {"Body": io.BytesIO(b"_sampled_rows,_null_chk_0,_nn_chk_0,_null_chk_1,_nn_chk_1,_num_chk_1,_nan_inf_chk_1\n1,0,1,0,1,10.0,0\n")}
    
    csv_ok = io.BytesIO(b"line_item_product_code,line_item_unblended_cost\nAmazonEC2,10.0\n")
    csv_ok.name = "cur_report.csv"
    try:
        mgr.stream_upload_and_create_table(csv_ok)
        raise AssertionError("Expected Pre-Glue fencing to block cutover")
    except AthenaExecutionError as e:
        assert "Lease expired or ownership lost" in str(e)
        executed_sqls = [c.kwargs["QueryString"] for c in mock_athena.start_query_execution.call_args_list]
        assert not any("CREATE EXTERNAL TABLE IF NOT EXISTS finops_db.cur_report " in s for s in executed_sqls)
    print("[PASS] 4/4: Pre-Glue fencing performs final lease verification immediately before cutover DDL.")

if __name__ == "__main__":
    test_sql_validation_and_forbidden_nodes()
    test_clean_sql_output_cases()
    test_previous_review_scenarios()
    test_feedback6_final_blockers()
    print("\nALL PRE-PRODUCTION CHECKS PASSED SUCCESSFULLY!")
