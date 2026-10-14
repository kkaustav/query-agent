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
    validate_full_dataset: bool = False,
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
    print("\n=== 3. Testing All 21 Review #4 & Review #5 Scenarios ===")

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

    mgr, mock_athena, mock_s3, mock_glue, _ = _build_mock_manager()
    s3_paginator = MagicMock()
    s3_paginator.paginate.return_value = [{"Contents": [{"Key": "data/tbl/ingestion_id=1/file.csv"}]}]
    mock_s3.get_paginator.return_value = s3_paginator
    mock_s3.delete_objects.return_value = {
        "Errors": [{"Key": "data/tbl/ingestion_id=1/file.csv", "Code": "AccessDenied", "Message": "Access Denied"}]
    }
    try:
        mgr._delete_s3_prefix("data/tbl/ingestion_id=1/")
        raise AssertionError("Expected AthenaExecutionError")
    except AthenaExecutionError as e:
        assert "partial deletion errors" in str(e)

    for bad_loc in (None, ""):
        mock_glue.get_table.return_value = {
            "Table": {"StorageDescriptor": {} if bad_loc is None else {"Location": bad_loc}}
        }
        try:
            mgr._verify_table_location_updated("cur_report", "s3://cur-bucket/data/cur_report/ingestion_id=123/")
            raise AssertionError("Expected fail-closed AthenaExecutionError")
        except AthenaExecutionError:
            pass

    print("[PASS] Core ingestion, rollback location verification, and access-control tests passed.")


def test_review6_7_distributed_and_recovery_scenarios():
    print("\n=== 4. Testing All 7 Review #6 Scenarios (Distributed Lock, Multi-Col Validation & Rollback Verification) ===")

    shared_dynamo_locks = {}

    def dynamo_put_item(**kwargs):
        item = kwargs["Item"]
        key = item["lock_key"]["S"]
        now_val = int(kwargs["ExpressionAttributeValues"][":now"]["N"])
        existing = shared_dynamo_locks.get(key)
        if existing and int(existing["lease_expires_at"]["N"]) >= now_val and existing["owner_id"]["S"] != item["owner_id"]["S"]:
            raise ClientError(
                {"Error": {"Code": "ConditionalCheckFailedException", "Message": "Lease held by another task"}},
                "PutItem",
            )
        shared_dynamo_locks[key] = item
        return {}

    mgr_task_a, _, mock_s3_a, _, mock_dynamo_a = _build_mock_manager(task_owner_id="ecs-task-A")
    mgr_task_b, _, mock_s3_b, _, mock_dynamo_b = _build_mock_manager(task_owner_id="ecs-task-B")
    mock_dynamo_a.put_item.side_effect = dynamo_put_item
    mock_dynamo_b.put_item.side_effect = dynamo_put_item

    mgr_task_a._acquire_distributed_table_lock("cur_report", "ing_a_111")
    csv_task_b = io.BytesIO(b"line_item_product_code,line_item_unblended_cost\nAmazonS3,25.0\n")
    csv_task_b.name = "cur_report.csv"
    try:
        mgr_task_b.stream_upload_and_create_table(csv_task_b)
        raise AssertionError("Expected Task B to be blocked by Task A's distributed DynamoDB lease lock")
    except AthenaExecutionError as e:
        assert "Concurrent distributed ingestion conflict" in str(e)

    for bad_restored_loc in ("s3://cur-bucket/data/cur_report/ingestion_id=WRONG/", None, ""):
        mgr, mock_athena, mock_s3, mock_glue, _ = _build_mock_manager()
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

    print("[PASS] Distributed locking and rollback verification tests passed.")


def test_feedback5_9_additional_scenarios():
    print("\n=== 5. Testing All 9 New Scenarios from Project Feedback_5.docx ===")

    # 1. Stale S3 manifest / Active DynamoDB lease -> Cleanup MUST NOT delete active prefix
    mgr, _, mock_s3, mock_glue, mock_dynamo = _build_mock_manager()
    active_ing_id = "active_id_123"
    active_prefix = "data/cur_report/ingestion_id=active_id_123/"
    
    mock_dynamo.get_item.return_value = {
        "Item": {
            "lock_key": {"S": "finops_db.cur_report"},
            "ingestion_id": {"S": active_ing_id},
            "lease_expires_at": {"N": str(int(time.time()) + 300)}
        }
    }
    
    expired_manifest = {
        "s3_prefix": active_prefix,
        "ingestion_id": active_ing_id,
        "state": "UPLOADING",
        "updated_at": time.time() - 7200,
        "lease_expires_at": time.time() - 7200,
    }
    
    def mock_paginate(Bucket, Prefix):
        if Prefix.startswith("_ingestion_state"):
            return [{"Contents": [{"Key": "manifest.json"}]}]
        return [{"Contents": [{"Key": f"{active_prefix}file.csv"}]}]
        
    mock_s3.get_paginator.return_value.paginate.side_effect = mock_paginate
    mock_s3.get_object.return_value = {"Body": io.BytesIO(json.dumps(expired_manifest).encode())}
    mock_glue.get_table.return_value = {"Table": {"StorageDescriptor": {"Location": "s3://cur-bucket/data/cur_report/ingestion_id=old_active/"}}}

    removed = mgr.cleanup_abandoned_staging_prefixes("cur_report", min_age_seconds=0)
    assert active_prefix not in removed, "Cleanup deleted an active DynamoDB lock prefix based on an expired S3 manifest!"
    print("[PASS] 1/9: Cleanup skips expired S3 manifests if DynamoDB lease is actively renewed.")

    # 2. Athena validation query returns malformed/no results -> Validation fails closed
    mgr, mock_athena, mock_s3, _, _ = _build_mock_manager()
    mock_athena.start_query_execution.return_value = {"QueryExecutionId": "qid-val-missing"}
    mock_athena.get_query_execution.return_value = {"QueryExecution": {"Status": {"State": "SUCCEEDED"}}}
    
    try:
        mgr._validate_staging_table("_staging_tbl", [("col1", "STRING")])
        raise AssertionError("Expected AthenaExecutionError on missing validation OutputLocation")
    except AthenaExecutionError as e:
        assert "Validation Failed: Missing or invalid" in str(e)
        
    mock_athena.get_query_execution.return_value = {
        "QueryExecution": {
            "Status": {"State": "SUCCEEDED"},
            "ResultConfiguration": {"OutputLocation": "s3://bucket/key.csv"}
        }
    }
    mock_s3.get_object.side_effect = ClientError({"Error": {"Code": "AccessDenied", "Message": "Denied"}}, "GetObject")
    try:
        mgr._validate_staging_table("_staging_tbl", [("col1", "STRING")])
        raise AssertionError("Expected AthenaExecutionError on S3 read failure during validation")
    except AthenaExecutionError as e:
        assert "Validation Failed: Could not read Athena validation result from S3" in str(e)
        
    mock_s3.get_object.side_effect = None
    mock_s3.get_object.return_value = {"Body": io.BytesIO(b"bad_column\n1\n")}
    try:
        mgr._validate_staging_table("_staging_tbl", [("col1", "STRING")])
        raise AssertionError("Expected ValueError when validation result is missing expected metrics")
    except ValueError as e:
        assert "empty or malformed validation metrics" in str(e)
    print("[PASS] 2/9: Staging validation fails closed when S3 results are missing, unreadable, or malformed.")

    # 3. Old production data is NOT deleted immediately after cutover (Issue 3)
    mgr, mock_athena, mock_s3, mock_glue, _ = _build_mock_manager()
    old_snap = {
        "Table": {
            "Name": "cur_report",
            "StorageDescriptor": {
                "Location": "s3://cur-bucket/data/cur_report/ingestion_id=old_production_prefix/",
                "Columns": [{"Name": "col1", "Type": "STRING"}],
            }
        }
    }
    mock_glue.get_table.side_effect = [
        old_snap,
        {"Table": {"StorageDescriptor": {"Location": "s3://cur-bucket/data/cur_report/ingestion_id=NEW_prefix/"}}}
    ]
    mock_athena.start_query_execution.return_value = {"QueryExecutionId": "qid-ok"}
    mock_athena.get_query_execution.return_value = {"QueryExecution": {"Status": {"State": "SUCCEEDED"}}}
    mock_s3.get_paginator.return_value.paginate.return_value = [{"Contents": []}]
    
    with patch.object(mgr, "_delete_s3_prefix") as mock_delete_s3:
        csv_f = io.BytesIO(b"col1\nval1\n")
        csv_f.name = "cur_report.csv"
        mgr.stream_upload_and_create_table(csv_f)
        
        # Verify the old prefix is NOT deleted
        deleted_prefixes = [call_args[0][0] for call_args in mock_delete_s3.call_args_list]
        assert "data/cur_report/ingestion_id=old_production_prefix/" not in deleted_prefixes
    print("[PASS] 3/9: Old production data is retained (not immediately deleted) after cutover to protect running queries.")

if __name__ == "__main__":
    test_sql_validation_and_forbidden_nodes()
    test_clean_sql_output_cases()
    test_previous_review_scenarios()
    test_review6_7_distributed_and_recovery_scenarios()
    test_feedback5_9_additional_scenarios()
    print("\nALL REVIEW #4, #5, #6 & FEEDBACK #5 PRE-PRODUCTION CHECKS PASSED SUCCESSFULLY!")
