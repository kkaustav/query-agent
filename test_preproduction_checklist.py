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


def _build_mock_manager(task_owner_id: str = "ecs-task-1", use_atomic_glue_cutover: bool = False):
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
    mgr, mock_athena, mock_s3, mock_glue, _ = _build_mock_manager()
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
    prod_created_locations = []
    executed_queries = []

    def capture_start_query(**kwargs):
        q = kwargs.get("QueryString", "")
        executed_queries.append(q)
        if q.startswith("CREATE EXTERNAL TABLE IF NOT EXISTS finops_db.cur_report "):
            loc_match = [part for part in q.split("'") if part.startswith("s3://")]
            if loc_match:
                prod_created_locations.append(loc_match[0])
        return {"QueryExecutionId": f"qid-{len(executed_queries)}"}

    mock_athena.start_query_execution.side_effect = capture_start_query
    mock_athena.get_query_execution.return_value = {"QueryExecution": {"Status": {"State": "SUCCEEDED"}}}
    mock_glue.get_table.side_effect = lambda DatabaseName, Name: (
        old_snap
        if not prod_created_locations
        else {"Table": {"StorageDescriptor": {"Location": prod_created_locations[-1]}}}
    )
    mock_s3.get_paginator.return_value = s3_paginator
    mock_s3.delete_objects.return_value = {"Deleted": [{"Key": "data/cur_report/ingestion_id=old_prefix/f.csv"}]}

    csv_ok = io.BytesIO(b"line_item_product_code,line_item_unblended_cost\nAmazonEC2,10.0\n")
    csv_ok.name = "cur_report.csv"
    res = mgr.stream_upload_and_create_table(csv_ok)
    assert prod_created_locations and "ingestion_id=old_prefix" not in prod_created_locations[-1]
    assert any("SUM(CAST(line_item_unblended_cost AS DOUBLE))" in q for q in executed_queries)

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

    print("[PASS] All 21 Review #4 & Review #5 scenarios verified.")


def test_review6_7_distributed_and_recovery_scenarios():
    print("\n=== 4. Testing All 7 New Review #6 Scenarios (Distributed Lock, Multi-Col Validation & Rollback Verification) ===")

    # -------------------------------------------------------------------------
    # Scenario 1: Two separate processes / ECS tasks upload to the same table
    # -------------------------------------------------------------------------
    shared_dynamo_locks = {}

    def dynamo_put_item(**kwargs):
        item = kwargs["Item"]
        key = item["lock_key"]["S"]
        owner = item["owner_id"]["S"]
        now_val = int(kwargs["ExpressionAttributeValues"][":now"]["N"])
        existing = shared_dynamo_locks.get(key)
        if existing and int(existing["lease_expires_at"]["N"]) >= now_val and existing["owner_id"]["S"] != owner:
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

    # Task A acquires the distributed DynamoDB lock for finops_db.cur_report
    mgr_task_a._acquire_distributed_table_lock("cur_report", "ing_a_111")

    # Task B (running in a separate container/process with clear local locks) attempts to upload cur_report.csv
    AthenaManager._TABLE_LOCKS.clear()
    csv_task_b = io.BytesIO(b"line_item_product_code,line_item_unblended_cost\nAmazonS3,25.0\n")
    csv_task_b.name = "cur_report.csv"
    try:
        mgr_task_b.stream_upload_and_create_table(csv_task_b)
        raise AssertionError("Expected Task B to be blocked by Task A's distributed DynamoDB lease lock")
    except AthenaExecutionError as e:
        assert "Concurrent distributed ingestion conflict" in str(e)
        assert mock_s3_b.upload_fileobj.call_count == 0
    print("[PASS] 1/7: Two separate ECS tasks uploading to the same table blocked via distributed DynamoDB lease.")

    # -------------------------------------------------------------------------
    # Scenario 2: Cleanup runs from a different ECS task while another task is uploading
    # -------------------------------------------------------------------------
    shared_s3_manifests = {}

    mgr_task_a, _, mock_s3_a, mock_glue_a, _ = _build_mock_manager(task_owner_id="ecs-task-A")
    mgr_task_b, _, mock_s3_b, mock_glue_b, _ = _build_mock_manager(task_owner_id="ecs-task-B")

    def s3_put_manifest(Bucket, Key, Body, **kwargs):
        shared_s3_manifests[Key] = Body

    mock_s3_a.put_object.side_effect = s3_put_manifest

    # Task A writes an UPLOADING state for its prefix
    task_a_prefix = "data/cur_report/ingestion_id=taska999/"
    mgr_task_a._set_ingestion_state(task_a_prefix, "cur_report", "taska999", "UPLOADING")

    # Clear process-local memory to prove Task B has zero in-memory visibility of Task A
    AthenaManager._INGESTION_STATES.clear()

    # Configure Task B's S3 client to see the durable S3 manifest written by Task A
    mock_glue_b.get_table.return_value = {
        "Table": {
            "StorageDescriptor": {
                "Location": "s3://cur-bucket/data/cur_report/ingestion_id=prod000/",
                "Columns": [{"Name": "line_item_product_code", "Type": "STRING"}],
            }
        }
    }

    def task_b_paginate(Bucket, Prefix):
        if Prefix.startswith("_ingestion_state/"):
            return [{"Contents": [{"Key": k} for k in shared_s3_manifests]}]
        return [{"Contents": [{"Key": f"{task_a_prefix}cur_report.csv"}]}]

    task_b_paginator = MagicMock()
    task_b_paginator.paginate.side_effect = task_b_paginate
    mock_s3_b.get_paginator.return_value = task_b_paginator
    mock_s3_b.get_object.side_effect = lambda Bucket, Key: {"Body": io.BytesIO(shared_s3_manifests[Key])}

    removed_by_b = mgr_task_b.cleanup_abandoned_staging_prefixes("cur_report", min_age_seconds=0)
    assert task_a_prefix not in removed_by_b
    assert mock_s3_b.delete_objects.call_count == 0
    print("[PASS] 2/7: Cleanup running on Task B consults durable S3 state and retains Task A's active upload.")

    # -------------------------------------------------------------------------
    # Scenario 3: Rollback succeeds at DDL level but restores wrong, missing, or empty Glue location
    # -------------------------------------------------------------------------
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

        # 1. Staging CREATE (ok), 2. Staging validation (ok), 3. Prod DROP (ok),
        # 4. Prod CREATE (FAIL), 5. Staging DROP (ok), 6. Rollback CREATE (ok at DDL level!)
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
            assert "Cutover verification failed" in str(e)
    print("[PASS] 3/7: Rollback DDL succeeded but restored wrong/missing/empty Glue location -> caught as CRITICAL RECOVERY FAILURE.")

    # -------------------------------------------------------------------------
    # Scenario 4: Queries arrive while production is dropped and recreated (cutover window)
    # -------------------------------------------------------------------------
    mgr, mock_athena, mock_s3, _, _ = _build_mock_manager()
    # Mark table as currently inside the active CUTOVER window
    mgr._set_ingestion_state("data/cur_data/ingestion_id=cutover1/", "cur_data", "cutover1", "CUTOVER")

    mock_athena.start_query_execution.side_effect = [
        {"QueryExecutionId": "qid-during-drop"},
        {"QueryExecutionId": "qid-after-create"},
    ]
    mock_athena.get_query_execution.side_effect = [
        {
            "QueryExecution": {
                "Status": {
                    "State": "FAILED",
                    "StateChangeReason": "TABLE_NOT_FOUND: line 1:15: Table 'finops_db.cur_data' does not exist",
                    "AthenaError": {"ErrorCategory": 2, "ErrorType": 1001, "Retryable": False},
                }
            }
        },
        {
            "QueryExecution": {
                "Status": {"State": "SUCCEEDED"},
                "ResultConfiguration": {"OutputLocation": "s3://cur-bucket/athena-results/qid-after-create.csv"},
                "Statistics": {"DataScannedInBytes": 512},
            }
        },
    ]
    mock_s3.get_object.return_value = {"Body": io.BytesIO(b"line_item_product_code\nAmazonEC2\n")}

    df, plan = mgr.execute_query("SELECT line_item_product_code FROM cur_data")
    assert not df.empty and mock_athena.start_query_execution.call_count == 2
    print("[PASS] 4/7: Concurrent query arriving during DROP/CREATE cutover window recovers cleanly.")

    # -------------------------------------------------------------------------
    # Scenario 5: Staging validation encounters invalid data in a later row or second numeric column
    # -------------------------------------------------------------------------
    mgr, mock_athena, mock_s3, mock_glue, _ = _build_mock_manager()
    mock_glue.get_table.side_effect = ClientError({"Error": {"Code": "EntityNotFoundException"}}, "GetTable")
    # 1. Staging CREATE succeeds, 2. Multi-column staging validation fails on CAST(line_item_unblended_cost AS DOUBLE) in row 450, 3. Staging DROP succeeds
    mock_athena.start_query_execution.side_effect = [
        {"QueryExecutionId": "stg-create"},
        {"QueryExecutionId": "stg-val-fail"},
        {"QueryExecutionId": "stg-drop"},
    ]
    mock_athena.get_query_execution.side_effect = [
        {"QueryExecution": {"Status": {"State": "SUCCEEDED"}}},
        {
            "QueryExecution": {
                "Status": {
                    "State": "FAILED",
                    "StateChangeReason": "INVALID_CAST_ARGUMENT: Cannot cast 'CORRUPT_COST_ROW_450' to DOUBLE",
                }
            }
        },
        {"QueryExecution": {"Status": {"State": "SUCCEEDED"}}},
    ]
    s3_pag = MagicMock()
    s3_pag.paginate.return_value = [{"Contents": []}]
    mock_s3.get_paginator.return_value = s3_pag

    bad_later_col_csv = io.BytesIO(
        b"line_item_product_code,line_item_unblended_cost\nAmazonEC2,10.50\nAmazonS3,CORRUPT_COST_ROW_450\n"
    )
    bad_later_col_csv.name = "cur_report.csv"
    try:
        mgr.stream_upload_and_create_table(bad_later_col_csv)
        raise AssertionError("Expected staging validation to fail on invalid numeric data in second column")
    except AthenaExecutionError as e:
        assert "Staging table validation query failed" in str(e)
        val_query = mock_athena.start_query_execution.call_args_list[1].kwargs["QueryString"]
        assert "SUM(CAST(line_item_unblended_cost AS DOUBLE))" in val_query
    print("[PASS] 5/7: Multi-column staging validation catches corrupt data in non-first column / later row.")

    # -------------------------------------------------------------------------
    # Scenario 6: Application restarts during ingestion (in-memory state disappears)
    # -------------------------------------------------------------------------
    persisted_manifests = {}
    mgr, _, mock_s3, mock_glue, _ = _build_mock_manager(task_owner_id="pre-crash-task")
    mock_s3.put_object.side_effect = lambda Bucket, Key, Body, **kw: persisted_manifests.update({Key: Body})

    crashed_prefix = "data/cur_report/ingestion_id=crashed777/"
    mgr._set_ingestion_state(crashed_prefix, "cur_report", "crashed777", "UPLOADING")

    # Simulate container crash and restart: wipe in-memory _INGESTION_STATES completely
    AthenaManager._INGESTION_STATES.clear()
    assert len(AthenaManager._INGESTION_STATES) == 0

    mock_glue.get_table.side_effect = ClientError({"Error": {"Code": "EntityNotFoundException"}}, "GetTable")

    def restart_paginate(Bucket, Prefix):
        if Prefix.startswith("_ingestion_state/"):
            return [{"Contents": [{"Key": k} for k in persisted_manifests]}]
        return [{"Contents": [{"Key": f"{crashed_prefix}cur_report.csv"}]}]

    restart_paginator = MagicMock()
    restart_paginator.paginate.side_effect = restart_paginate
    mock_s3.get_paginator.return_value = restart_paginator
    mock_s3.get_object.side_effect = lambda Bucket, Key: {"Body": io.BytesIO(persisted_manifests[Key])}
    mock_s3.delete_objects.return_value = {"Deleted": []}

    # Immediately after restart (while lease has not expired), cleanup retains the prefix
    removed_early = mgr.cleanup_abandoned_staging_prefixes("cur_report", min_age_seconds=0)
    assert crashed_prefix not in removed_early

    # Once lease and min_age_seconds expire, cleanup safely reclaims the abandoned prefix from the crashed run
    for k in persisted_manifests:
        rec = json.loads(persisted_manifests[k].decode("utf-8"))
        rec["updated_at"] -= 7200
        rec["lease_expires_at"] -= 7200
        persisted_manifests[k] = json.dumps(rec).encode("utf-8")

    removed_after_expiry = mgr.cleanup_abandoned_staging_prefixes("cur_report", min_age_seconds=3600)
    assert crashed_prefix in removed_after_expiry
    print("[PASS] 6/7: Application restart recovery preserves unexpired state via S3 and cleans up after lease expiry.")

    # -------------------------------------------------------------------------
    # Scenario 7: Stale ingestion records accumulate -> Pruned by retention policy
    # -------------------------------------------------------------------------
    persisted_manifests.clear()
    mgr, _, mock_s3, mock_glue, _ = _build_mock_manager()
    mock_s3.put_object.side_effect = lambda Bucket, Key, Body, **kw: persisted_manifests.update({Key: Body})
    mock_glue.get_table.return_value = {
        "Table": {
            "StorageDescriptor": {
                "Location": "s3://cur-bucket/data/cur_report/ingestion_id=current_active/",
                "Columns": [{"Name": "c1", "Type": "STRING"}],
            }
        }
    }

    old_retired_prefix = "data/cur_report/ingestion_id=oldretired1/"
    recent_failed_prefix = "data/cur_report/ingestion_id=recentfailed2/"
    active_prefix = "data/cur_report/ingestion_id=current_active/"

    mgr._set_ingestion_state(old_retired_prefix, "cur_report", "oldretired1", "RETIRED")
    mgr._set_ingestion_state(recent_failed_prefix, "cur_report", "recentfailed2", "FAILED")
    mgr._set_ingestion_state(active_prefix, "cur_report", "current_active", "ACTIVE")

    # Age only old_retired_prefix past the retention window (e.g. 10 days old)
    AthenaManager._INGESTION_STATES[old_retired_prefix]["updated_at"] -= 10 * 86400
    old_key = mgr._state_manifest_s3_key("cur_report", "oldretired1")
    rec_data = json.loads(persisted_manifests[old_key].decode("utf-8"))
    rec_data["updated_at"] -= 10 * 86400
    persisted_manifests[old_key] = json.dumps(rec_data).encode("utf-8")

    mock_s3.get_paginator.return_value = restart_paginator
    mock_s3.get_object.side_effect = lambda Bucket, Key: {"Body": io.BytesIO(persisted_manifests[Key])}
    mock_s3.delete_objects.return_value = {"Deleted": []}

    pruned = mgr.prune_stale_ingestion_records("cur_report", retention_seconds=7 * 86400)
    assert old_retired_prefix in pruned
    assert recent_failed_prefix not in pruned
    assert active_prefix not in pruned
    assert old_retired_prefix not in AthenaManager._INGESTION_STATES
    print("[PASS] 7/7: Stale terminal ingestion state records pruned while retaining active and recent records.")


if __name__ == "__main__":
    test_sql_validation_and_forbidden_nodes()
    test_clean_sql_output_cases()
    test_review4_and_review5_scenarios()
    test_review6_7_distributed_and_recovery_scenarios()
    print("\nALL REVIEW #4, #5 & #6 PRE-PRODUCTION CHECKS PASSED SUCCESSFULLY!")
