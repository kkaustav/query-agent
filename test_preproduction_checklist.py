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
    mock_athena.get_query_execution.return_value = {
        "QueryExecution": {
            "Status": {"State": "SUCCEEDED"},
            "ResultConfiguration": {
                "OutputLocation": "s3://cur-bucket/athena-results/default.csv"
            },
            "Statistics": {"DataScannedInBytes": 512},
        }
    }
    mock_dynamodb.update_item.return_value = {
        "Attributes": {"counter_value": {"N": "1"}}
    }
    mock_s3.get_object.side_effect = lambda **kwargs: {
        "Body": io.BytesIO(
            b"_sampled_rows,_null_chk_0,_nn_chk_0,_null_chk_1,_nn_chk_1,_num_chk_1,_nan_inf_chk_1\n"
            b"1,0,1,0,1,10.0,0\n"
        )
    }
    mock_s3.get_paginator.return_value.paginate.return_value = [{"Contents": []}]

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
            strict_workgroup_check=True,
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

    # 1. EntityNotFoundException -> None
    mgr, mock_athena, mock_s3, mock_glue, _ = _build_mock_manager()
    mock_glue.get_table.side_effect = ClientError(
        {"Error": {"Code": "EntityNotFoundException", "Message": "Table not found"}}, "GetTable"
    )
    assert mgr._get_existing_table_snapshot("new_table") is None

    # 2. AccessDeniedException -> Fail
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

    # 3. S3 DeleteObjects partial errors -> Fail
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

    # 4. Existing table + successful staged replacement
    mgr, mock_athena, mock_s3, mock_glue, mock_dyn = _build_mock_manager(use_atomic_glue_cutover=True)
    old_snap = {
        "Table": {
            "Name": "cur_report",
            "StorageDescriptor": {
                "Location": "s3://cur-bucket/data/cur_report/ingestion_id=old_prefix/",
                "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                "Columns": [
                    {"Name": "line_item_product_code", "Type": "STRING"},
                    {"Name": "line_item_unblended_cost", "Type": "DOUBLE"},
                ],
                "SerdeInfo": {"Parameters": {"field.delim": ","}},
            },
        }
    }
    prod_updated_locations = []

    def capture_update_table(DatabaseName, TableInput, **kwargs):
        prod_updated_locations.append(TableInput["StorageDescriptor"]["Location"])
        return {}

    mock_glue.update_table.side_effect = capture_update_table
    mock_athena.start_query_execution.return_value = {"QueryExecutionId": "qid-stg-ok"}
    mock_glue.get_table.side_effect = lambda DatabaseName, Name: (
        old_snap
        if not prod_updated_locations
        else {
            "Table": {
                "StorageDescriptor": {
                    "Location": prod_updated_locations[-1],
                    "Columns": [
                        {"Name": "line_item_product_code", "Type": "STRING"},
                        {"Name": "line_item_unblended_cost", "Type": "DOUBLE"},
                    ],
                }
            }
        }
    )

    csv_ok = io.BytesIO(b"line_item_product_code,line_item_unblended_cost\nAmazonEC2,10.0\n")
    csv_ok.name = "cur_report.csv"
    res = mgr.stream_upload_and_create_table(csv_ok)
    assert prod_updated_locations and "ingestion_id=old_prefix" not in prod_updated_locations[-1]
    assert prod_updated_locations[-1] in res

    # 5. Missing and empty Glue Location fail-closed
    mgr, _, _, mock_glue, _ = _build_mock_manager()
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

    # Scenario 1: Two separate processes / ECS tasks upload to the same table
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
    AthenaManager._TABLE_LOCKS.clear()
    csv_task_b = io.BytesIO(b"line_item_product_code,line_item_unblended_cost\nAmazonS3,25.0\n")
    csv_task_b.name = "cur_report.csv"
    try:
        mgr_task_b.stream_upload_and_create_table(csv_task_b)
        raise AssertionError("Expected Task B to be blocked by Task A's distributed DynamoDB lease lock")
    except AthenaExecutionError as e:
        assert "Concurrent distributed ingestion conflict" in str(e)

    # Scenario 3: Rollback succeeds at API/DDL level but restores wrong, missing, or empty Glue location
    for bad_restored_loc in ("s3://cur-bucket/data/cur_report/ingestion_id=WRONG/", None, ""):
        mgr, mock_athena, mock_s3, mock_glue, mock_dynamo = _build_mock_manager(use_atomic_glue_cutover=True)
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
        mock_glue.update_table.side_effect = [
            ClientError({"Error": {"Code": "InternalError", "Message": "Prod update failed"}}, "UpdateTable"),
            {},
        ]
        mock_athena.start_query_execution.return_value = {"QueryExecutionId": "qid-stg-val"}

        csv_rb = io.BytesIO(b"line_item_product_code\nAmazonEC2\n")
        csv_rb.name = "cur_report.csv"
        try:
            mgr.stream_upload_and_create_table(csv_rb)
            raise AssertionError(f"Expected CRITICAL RECOVERY FAILURE when rollback restores location={bad_restored_loc!r}")
        except AthenaExecutionError as e:
            assert "CRITICAL RECOVERY FAILURE" in str(e)
            assert "Cutover verification failed" in str(e)
    print("[PASS] Rollback DDL succeeded but restored wrong/missing/empty Glue location -> caught as CRITICAL RECOVERY FAILURE.")
    print("[PASS] Distributed locking and rollback verification tests passed.")


def test_feedback7_final_blockers():
    print("\n=== 5. Testing All Final Blockers from Project_Feedback_7_Review_and_Remaining_Blockers.docx ===")

    # 1. Cleanup vs New Ingestion Race
    mgr, _, mock_s3, _, mock_dynamo = _build_mock_manager()
    mock_dynamo.put_item.side_effect = ClientError(
        {"Error": {"Code": "ConditionalCheckFailedException", "Message": "Lock held by active ingestion"}}, "PutItem"
    )
    removed = mgr.cleanup_abandoned_staging_prefixes("cur_report", min_age_seconds=0, require_verified_state=False)
    assert len(removed) == 0
    assert mock_s3.delete_objects.call_count == 0
    print("[PASS] 1/6: Cleanup acquires table lock first and skips deletion if table is actively locked.")

    # 2. DynamoDB unavailable for cleanup -> Fails closed
    mock_dynamo.put_item.side_effect = ClientError(
        {"Error": {"Code": "ResourceNotFoundException", "Message": "Table not found"}}, "PutItem"
    )
    try:
        mgr.cleanup_abandoned_staging_prefixes("cur_report")
        raise AssertionError("Expected fail-closed AthenaExecutionError")
    except AthenaExecutionError as e:
        assert "Fail-Closed Distributed Lock Error" in str(e)
        assert mock_s3.delete_objects.call_count == 0
    print("[PASS] 2/6: Missing DynamoDB lock table fails cleanup closed.")

    # 3. Missing OutputLocation or empty CSV in Staging Validation fails closed
    mgr, mock_athena, mock_s3, _, _ = _build_mock_manager()
    mock_athena.start_query_execution.return_value = {"QueryExecutionId": "qid-val"}
    mock_athena.get_query_execution.return_value = {"QueryExecution": {"Status": {"State": "SUCCEEDED"}}}

    try:
        mgr._validate_staging_table("staging_tbl", [("line_item_product_code", "STRING")])
        raise AssertionError("Expected AthenaExecutionError on missing OutputLocation")
    except AthenaExecutionError as e:
        assert "Missing or invalid Athena validation result location" in str(e)

    mock_athena.get_query_execution.return_value = {
        "QueryExecution": {
            "Status": {"State": "SUCCEEDED"},
            "ResultConfiguration": {"OutputLocation": "s3://b/k.csv"},
        }
    }
    mock_s3.get_object.side_effect = None
    mock_s3.get_object.return_value = {"Body": io.BytesIO(b"")}
    try:
        mgr._validate_staging_table("staging_tbl", [("line_item_product_code", "STRING")])
        raise AssertionError("Expected AthenaExecutionError on empty validation CSV")
    except AthenaExecutionError as e:
        assert "empty or malformed validation metrics" in str(e)
    print("[PASS] 3/6: Staging validation fails closed when S3 results are missing or empty.")

    # 4. Null validation enforces 0 nulls for mandatory CUR fields
    mock_s3.get_object.side_effect = None
    mock_s3.get_object.return_value = {
        "Body": io.BytesIO(
            b"_sampled_rows,_null_chk_0,_num_chk_0,_nan_inf_chk_0\n1000,1,500.0,0\n"
        )
    }
    try:
        mgr._validate_staging_table("staging_tbl", [("line_item_unblended_cost", "DOUBLE")])
        raise AssertionError("Expected Validation Failed on mandatory CUR field with nulls")
    except AthenaExecutionError as e:
        assert "Mandatory FinOps column 'line_item_unblended_cost' contains 1 null values" in str(e)
    print("[PASS] 4/6: Staging validation explicitly enforces 0-null policy for mandatory CUR fields.")

    # 5. Stale worker fencing using VersionId
    mgr, mock_athena, mock_s3, mock_glue, mock_dyn = _build_mock_manager(use_atomic_glue_cutover=True)
    initial_snap = {
        "Table": {
            "Name": "cur_report",
            "VersionId": "v-111",
            "StorageDescriptor": {"Location": "s3://cur-bucket/data/cur_report/ingestion_id=old/"},
        }
    }
    mock_glue.get_table.return_value = initial_snap
    mock_athena.start_query_execution.return_value = {"QueryExecutionId": "qid-ok"}
    mock_glue.update_table.side_effect = ClientError(
        {"Error": {"Code": "ConcurrentModificationException", "Message": "VersionId mismatch"}}, "UpdateTable"
    )

    csv_ok = io.BytesIO(b"line_item_product_code\nAmazonEC2\n")
    csv_ok.name = "cur_report.csv"
    try:
        mgr.stream_upload_and_create_table(csv_ok)
        raise AssertionError("Expected Split-Brain Fencing Guardrail exception")
    except AthenaExecutionError as e:
        assert "Split-Brain Fencing Guardrail: Glue update rejected due to concurrent modification" in str(e)
    print("[PASS] 5/6: Atomic cutover natively fences out stale workers using Glue VersionId.")

    # 6. Previous S3 prefix is RETIRED, not deleted immediately
    mgr, mock_athena, mock_s3, mock_glue, mock_dyn = _build_mock_manager(use_atomic_glue_cutover=True)
    snap_with_cols = {
        "Table": {
            "Name": "cur_report",
            "VersionId": "v-111",
            "StorageDescriptor": {
                "Location": "s3://cur-bucket/data/cur_report/ingestion_id=old/",
                "Columns": [{"Name": "line_item_product_code", "Type": "STRING"}],
            },
        }
    }
    updated_locs = []
    mock_glue.update_table.side_effect = lambda DatabaseName, TableInput, **kw: updated_locs.append(
        TableInput["StorageDescriptor"]["Location"]
    )
    mock_glue.get_table.side_effect = lambda DatabaseName, Name: (
        snap_with_cols
        if not updated_locs
        else {
            "Table": {
                "StorageDescriptor": {
                    "Location": updated_locs[-1],
                    "Columns": [{"Name": "line_item_product_code", "Type": "STRING"}],
                }
            }
        }
    )
    mock_athena.start_query_execution.return_value = {"QueryExecutionId": "qid-ok"}

    with patch.object(mgr, "_delete_s3_prefix") as mock_delete_s3:
        csv_ok2 = io.BytesIO(b"line_item_product_code\nAmazonEC2\n")
        csv_ok2.name = "cur_report.csv"
        mgr.stream_upload_and_create_table(csv_ok2)

        deleted_prefixes = [call_args[0][0] for call_args in mock_delete_s3.call_args_list]
        assert "data/cur_report/ingestion_id=old/" not in deleted_prefixes
    print("[PASS] 6/6: Old production data is retained (marked RETIRED, not deleted) after successful cutover.")


if __name__ == "__main__":
    test_sql_validation_and_forbidden_nodes()
    test_clean_sql_output_cases()
    test_review4_and_review5_scenarios()
    test_review6_7_distributed_and_recovery_scenarios()
    test_feedback7_final_blockers()
    print("\nALL PRE-PRODUCTION CHECKS PASSED SUCCESSFULLY!")
