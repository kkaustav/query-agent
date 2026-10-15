import io
import multiprocessing
import os
import re
import time
import boto3
from athena_manager import AthenaManager
from database import AthenaExecutionError

REGION = "us-east-1"
LOCK_TABLE = "nl_bi_finops_ingestion_locks"
WORKGROUP = "primary"


def _discover_bucket_and_db():
    """Discovers the project S3 bucket and Glue database from app.py or live AWS account."""
    db_name = "finops_db"
    bucket_name = None

    if os.path.exists("app.py"):
        with open("app.py", "r", encoding="utf-8") as f:
            content = f.read()
        b_match = re.search(r'BUCKET_NAME\s*=\s*["\']([a-z0-9.-]+)["\']', content)
        d_match = re.search(r'DATABASE_NAME\s*=\s*["\']([a-zA-Z0-9_]+)["\']', content)
        if b_match:
            bucket_name = b_match.group(1)
        if d_match:
            db_name = d_match.group(1)

    s3 = boto3.client("s3", region_name=REGION)
    buckets = [b["Name"] for b in s3.list_buckets().get("Buckets", [])]
    if not bucket_name or bucket_name not in buckets:
        if not buckets:
            raise RuntimeError("No S3 buckets found in AWS account for live integration test.")
        bucket_name = buckets[0]

    glue = boto3.client("glue", region_name=REGION)
    try:
        glue.get_database(Name=db_name)
    except glue.exceptions.EntityNotFoundException:
        glue.create_database(DatabaseInput={"Name": db_name})

    return db_name, bucket_name


def _multiprocess_lock_worker(db_name, bucket_name, table_name, start_barrier, result_queue):
    """Independent OS process worker that attempts to acquire a live DynamoDB lock."""
    mgr = AthenaManager(
        database=db_name,
        bucket_name=bucket_name,
        region=REGION,
        work_group=WORKGROUP,
        strict_workgroup_check=True,
        lock_table_name=LOCK_TABLE,
    )
    ing_id = f"mp_ing_{os.getpid()}"
    start_barrier.wait()
    try:
        token = mgr._acquire_distributed_table_lock(table_name, ing_id)
        time.sleep(1.5)
        mgr._release_distributed_table_lock(table_name, ing_id, fencing_token=token)
        result_queue.put(("ACQUIRED", os.getpid(), token))
    except AthenaExecutionError as e:
        result_queue.put(("BLOCKED", os.getpid(), str(e)))


def main():
    db_name, bucket_name = _discover_bucket_and_db()
    print(f"=== Live AWS Integration Suite (Region={REGION}, DB={db_name}, Bucket={bucket_name}) ===")

    # 1. Verify Live Athena Workgroup 100 MB Guardrail (strict_workgroup_check=True)
    mgr = AthenaManager(
        database=db_name,
        bucket_name=bucket_name,
        region=REGION,
        work_group=WORKGROUP,
        strict_workgroup_check=True,
        lock_table_name=LOCK_TABLE,
        use_atomic_glue_cutover=True,
        validate_full_dataset=True,
    )
    assert mgr.workgroup_guardrail_verified is True
    print("[PASS] 1/4 Live Athena Workgroup Guardrail: 'primary' verified with strict_workgroup_check=True (100 MB cutoff).")

    # 2. Verify Live DynamoDB Atomic Fencing Token Generation
    t1 = mgr._next_fencing_token()
    t2 = mgr._next_fencing_token()
    assert isinstance(t1, int) and isinstance(t2, int) and t2 > t1, f"Expected t2 > t1, got {t1}, {t2}"
    print(f"[PASS] 2/4 Live DynamoDB Atomic Fencing Counter: monotonically increased ({t1} -> {t2}).")

    # 3. Verify Independent OS-Process Distributed Lock Contention against Live DynamoDB
    ctx = multiprocessing.get_context("spawn")
    barrier = ctx.Barrier(2)
    q = ctx.Queue()
    lock_test_table = "live_mp_lock_test"

    p1 = ctx.Process(target=_multiprocess_lock_worker, args=(db_name, bucket_name, lock_test_table, barrier, q))
    p2 = ctx.Process(target=_multiprocess_lock_worker, args=(db_name, bucket_name, lock_test_table, barrier, q))
    p1.start()
    p2.start()
    p1.join(timeout=15)
    p2.join(timeout=15)

    outcomes = [q.get(timeout=5), q.get(timeout=5)]
    statuses = sorted([o[0] for o in outcomes])
    assert statuses == ["ACQUIRED", "BLOCKED"], f"Expected one ACQUIRED and one BLOCKED, got {outcomes}"
    print(f"[PASS] 3/4 Live Multi-Process DynamoDB Lock Contention (2 OS PIDs): {outcomes}")

    # 4. Live End-to-End S3 + Glue + Athena Ingestion, Full Staging Validation, Atomic VersionId Cutover & Query
    csv_v1 = io.BytesIO(
        b"line_item_product_code,line_item_unblended_cost\n"
        b"AmazonEC2,120.50\n"
        b"AmazonS3,45.25\n"
    )
    csv_v1.name = "live_cur_integration_test.csv"
    msg1 = mgr.stream_upload_and_create_table(csv_v1, validate_full_dataset=True)

    # Upload v2 to exercise existing snapshot + atomic Glue UpdateTable with VersionId fencing + RETIRED retention
    csv_v2 = io.BytesIO(
        b"line_item_product_code,line_item_unblended_cost\n"
        b"AmazonEC2,200.00\n"
        b"AmazonS3,50.00\n"
        b"AmazonRDS,75.00\n"
    )
    csv_v2.name = "live_cur_integration_test.csv"
    msg2 = mgr.stream_upload_and_create_table(csv_v2, validate_full_dataset=True)

    df, plan = mgr.execute_query(
        "SELECT line_item_product_code, SUM(line_item_unblended_cost) AS total_spend "
        "FROM live_cur_integration_test GROUP BY line_item_product_code ORDER BY total_spend DESC"
    )
    assert len(df) == 3 and float(df["total_spend"].iloc[0]) == 200.0, f"Unexpected query result:\n{df}"
    print(f"[PASS] 4/4 Live End-to-End S3/Glue/Athena Ingestion, Atomic Cutover & Query Verified ({plan}).")

    # Clean up disposable integration test table
    mgr._execute_ingestion_ddl(operation="drop", table_name="live_cur_integration_test")
    print("\nALL LIVE AWS INTEGRATION TESTS PASSED SUCCESSFULLY!")


if __name__ == "__main__":
    main()
