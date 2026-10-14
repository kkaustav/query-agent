import csv
import io
import logging
import os
import re
import tempfile
import time
import uuid
from typing import Dict, List, Optional, Tuple
import boto3
from boto3.s3.transfer import TransferConfig
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import sqlglot
from sqlglot import exp
from botocore.exceptions import ClientError

from database import validate_sql_query

logger = logging.getLogger(__name__)

# Authoritative schema overrides for standard AWS Cost & Usage Report (CUR) columns
CUR_COLUMN_TYPE_OVERRIDES: Dict[str, str] = {
    "line_item_unblended_cost": "DOUBLE",
    "line_item_blended_cost": "DOUBLE",
    "line_item_unblended_rate": "DOUBLE",
    "line_item_blended_rate": "DOUBLE",
    "line_item_usage_amount": "DOUBLE",
    "pricing_public_on_demand_cost": "DOUBLE",
    "pricing_public_on_demand_rate": "DOUBLE",
    "reservation_effective_cost": "DOUBLE",
    "savings_plan_savings_plan_effective_cost": "DOUBLE",
    "line_item_usage_start_date": "STRING",
    "line_item_usage_end_date": "STRING",
    "bill_billing_period_start_date": "STRING",
    "bill_billing_period_end_date": "STRING",
    "line_item_product_code": "STRING",
    "line_item_resource_id": "STRING",
    "line_item_usage_account_id": "STRING",
    "line_item_line_item_type": "STRING",
    "line_item_operation": "STRING",
    "line_item_usage_type": "STRING",
    "product_instance_type": "STRING",
    "product_product_name": "STRING",
    "product_region": "STRING",
}

MAX_EXCEL_UPLOAD_BYTES = 50 * 1024 * 1024  # 50 MB cap for in-memory Excel conversion


def configure_workgroup_for_deployment(
    work_group: str = "nl-bi-finops-workgroup",
    max_scan_mb: int = 100,
    region: str = "us-east-1",
) -> None:
    """
    One-time deployment/provisioning helper to configure the Athena Workgroup
    scan cutoff. Not called at application startup so the runtime IAM role
    only requires athena:GetWorkGroup rather than athena:UpdateWorkGroup.
    """
    athena = boto3.client("athena", region_name=region)
    athena.update_work_group(
        WorkGroup=work_group,
        ConfigurationUpdates={
            "EnforceWorkGroupConfiguration": True,
            "BytesScannedCutoffPerQuery": max_scan_mb * 1024 * 1024,
        },
    )


class AthenaManager:

    def __init__(
        self,
        database: str,
        bucket_name: str,
        region: str = "us-east-1",
        work_group: Optional[str] = None,
        max_scan_mb: int = 100,
        query_timeout_sec: int = 30,
        strict_workgroup_check: bool = True,
    ):
        if not re.fullmatch(r"[a-zA-Z_][a-zA-Z0-9_]*", database):
            raise ValueError(f"Invalid Glue database identifier: {database}")

        self.database = database
        self.bucket_name = bucket_name
        self.region = region
        self.work_group = work_group or os.getenv("ATHENA_WORKGROUP", "nl-bi-finops-workgroup")
        self.max_scan_mb = max_scan_mb
        self.max_scan_bytes = max_scan_mb * 1024 * 1024
        self.query_timeout_sec = query_timeout_sec
        self.strict_workgroup_check = strict_workgroup_check
        self.output_location = f"s3://{bucket_name}/athena-results/"
        self.workgroup_guardrail_verified = False

        self.athena = boto3.client("athena", region_name=region)
        self.s3 = boto3.client("s3", region_name=region)
        self.glue = boto3.client("glue", region_name=region)
        self.dialect = "trino"

        # Multipart streaming configuration (8 MB chunks) for CSV/TSV and Parquet uploads
        self.transfer_config = TransferConfig(
            multipart_threshold=8 * 1024 * 1024,
            multipart_chunksize=8 * 1024 * 1024,
            max_concurrency=4,
            use_threads=True,
        )

        # Read-only fail-closed verification of deployment-managed Workgroup scan cutoff
        self._verify_workgroup_guardrails(strict=self.strict_workgroup_check)

    def _verify_workgroup_guardrails(self, strict: bool = True) -> bool:
        """
        Verifies via get_work_group() that the target Athena Workgroup has
        EnforceWorkGroupConfiguration=True and a query-level BytesScannedCutoffPerQuery
        configured at or below self.max_scan_bytes (100 MB).
        Does not mutate AWS configuration at startup. Fails closed when strict=True.
        """
        try:
            wg_resp = self.athena.get_work_group(WorkGroup=self.work_group)
            wg_config = wg_resp.get("WorkGroup", {}).get("Configuration", {})
            actual_cutoff = wg_config.get("BytesScannedCutoffPerQuery")
            enforced = wg_config.get("EnforceWorkGroupConfiguration", False)

            self.workgroup_guardrail_verified = (
                isinstance(actual_cutoff, int)
                and 0 < actual_cutoff <= self.max_scan_bytes
                and enforced is True
            )
            if not self.workgroup_guardrail_verified:
                msg = (
                    f"Fail-Closed FinOps Guardrail Error: Athena Workgroup '{self.work_group}' "
                    f"does not have an enforced scan cutoff <= {self.max_scan_bytes} bytes. "
                    f"Got BytesScannedCutoffPerQuery={actual_cutoff}, EnforceWorkGroupConfiguration={enforced}."
                )
                logger.error(msg)
                if strict:
                    raise RuntimeError(msg)
            return self.workgroup_guardrail_verified
        except ClientError as e:
            msg = (
                f"Fail-Closed FinOps Guardrail Error: Unable to verify Athena Workgroup "
                f"'{self.work_group}' configuration via GetWorkGroup: {e}"
            )
            logger.error(msg)
            if strict:
                raise RuntimeError(msg) from e
            return False

    def _poll_athena_execution(self, query_execution_id: str) -> dict:
        """Polls an Athena QueryExecutionId with a hard timeout circuit breaker."""
        start_time = time.time()
        while True:
            if (time.time() - start_time) > self.query_timeout_sec:
                try:
                    self.athena.stop_query_execution(QueryExecutionId=query_execution_id)
                except Exception:
                    pass
                raise TimeoutError(
                    f"Athena query exceeded timeout limit of {self.query_timeout_sec}s and was cancelled."
                )

            status_resp = self.athena.get_query_execution(
                QueryExecutionId=query_execution_id
            )
            query_info = status_resp.get("QueryExecution", {})
            state = query_info.get("Status", {}).get("State")

            if state == "SUCCEEDED":
                return query_info
            elif state in ["FAILED", "CANCELLED"]:
                reason = query_info.get("Status", {}).get(
                    "StateChangeReason", "Unknown error"
                )
                # Classify SQL-level query errors (e.g. COLUMN_NOT_FOUND, SYNTAX_ERROR) as ValueError
                # so the agent can self-heal them, while infrastructure/cutoff errors raise RuntimeError
                sql_error_markers = (
                    "COLUMN_NOT_FOUND",
                    "TABLE_NOT_FOUND",
                    "SYNTAX_ERROR",
                    "TYPE_MISMATCH",
                    "FUNCTION_NOT_FOUND",
                    "INVALID_CAST_ARGUMENT",
                    "AMBIGUOUS_NAME",
                    "line ",
                )
                if state == "FAILED" and any(marker in reason for marker in sql_error_markers):
                    raise ValueError(f"Athena SQL Error: {reason}")
                raise RuntimeError(f"Athena Query {state}: {reason}")
            time.sleep(0.5)

    def execute_query(self, sql: str) -> Tuple[pd.DataFrame, str]:
        """
        Strictly read-only analytical query execution boundary for Amazon Athena.
        Query scan cost is bounded at the AWS Workgroup level via BytesScannedCutoffPerQuery,
        which cancels any query whose scanned bytes exceed the configured threshold.
        """
        if self.strict_workgroup_check and not self.workgroup_guardrail_verified:
            raise RuntimeError(
                f"Fail-Closed FinOps Guardrail: Refusing to execute Athena query because "
                f"Workgroup '{self.work_group}' {self.max_scan_mb} MB scan cutoff is not verified."
            )

        validate_sql_query(sql, dialect="trino")
        sql_clean = sql.strip().rstrip(";")

        response = self.athena.start_query_execution(
            QueryString=sql_clean,
            QueryExecutionContext={"Database": self.database},
            ResultConfiguration={"OutputLocation": self.output_location},
            WorkGroup=self.work_group,
        )
        query_execution_id = response["QueryExecutionId"]
        query_info = self._poll_athena_execution(query_execution_id)

        output_location = query_info.get("ResultConfiguration", {}).get(
            "OutputLocation", ""
        )
        if not output_location.endswith(".csv"):
            return pd.DataFrame(), f"Athena Execution ID: {query_execution_id}"

        s3_path = output_location.replace("s3://", "")
        target_bucket, result_key = s3_path.split("/", 1)

        try:
            obj = self.s3.get_object(Bucket=target_bucket, Key=result_key)
            df = pd.read_csv(obj["Body"])
        except (ClientError, pd.errors.EmptyDataError):
            df = pd.DataFrame()

        bytes_scanned = (
            query_info.get("Statistics", {}).get("DataScannedInBytes", 0)
        )
        plan_summary = (
            f"Athena Execution ID: {query_execution_id} "
            f"(Data Scanned: {bytes_scanned / (1024 * 1024):.2f} MB)"
        )
        return df, plan_summary

    def _execute_ingestion_ddl(
        self,
        operation: str,
        table_name: str,
        col_defs: Optional[List[Tuple[str, str]]] = None,
        storage_format: str = "PARQUET",
        s3_location: str = "",
        field_delimiter: str = ",",
    ) -> str:
        """
        Constructs and validates ingestion DDL exclusively from trusted, regex-verified
        identifiers and fixed templates, then verifies the full AST via sqlglot before execution.
        Note: In multi-user production, table registration should run under a separate
        ingestion service role rather than the query execution role.
        """
        if not re.fullmatch(r"[a-zA-Z_][a-zA-Z0-9_]*", self.database):
            raise ValueError(f"Invalid database identifier: {self.database}")
        if not re.fullmatch(r"[a-zA-Z_][a-zA-Z0-9_]*", table_name):
            raise ValueError(f"Invalid table identifier: {table_name}")

        allowed_types = {
            "STRING", "BIGINT", "INT", "DOUBLE", "FLOAT", "BOOLEAN", "DATE", "TIMESTAMP"
        }

        if operation == "drop":
            ddl_sql = f"DROP TABLE IF EXISTS {self.database}.{table_name}"
            parsed = sqlglot.parse(ddl_sql, read="hive", error_level=sqlglot.ErrorLevel.RAISE)
            if len(parsed) != 1 or not isinstance(parsed[0], exp.Drop):
                raise PermissionError("AST verification failed for constructed DROP TABLE DDL.")
            table_node = parsed[0].this
            if (
                not isinstance(table_node, exp.Table)
                or table_node.db.lower() != self.database.lower()
                or table_node.name.lower() != table_name.lower()
            ):
                raise PermissionError("AST target mismatch in constructed DROP TABLE DDL.")

        elif operation == "create":
            if not col_defs:
                raise ValueError("Column definitions are required for CREATE EXTERNAL TABLE.")
            expected_s3_prefix = f"s3://{self.bucket_name}/data/{table_name}/"
            if not s3_location.startswith(expected_s3_prefix) or "'" in s3_location or ";" in s3_location:
                raise PermissionError(f"Invalid S3 table location: {s3_location}")

            validated_cols = []
            for col_name, col_type in col_defs:
                if not re.fullmatch(r"[a-zA-Z_][a-zA-Z0-9_]*", col_name):
                    raise ValueError(f"Invalid column identifier: {col_name}")
                upper_type = col_type.upper()
                if upper_type not in allowed_types and not re.fullmatch(r"DECIMAL\(\d{1,2},\d{1,2}\)", upper_type):
                    raise ValueError(f"Unsupported Athena DDL column type: {col_type}")
                validated_cols.append(f"`{col_name}` {upper_type}")

            cols_sql = ", ".join(validated_cols)
            if storage_format == "PARQUET":
                ddl_sql = (
                    f"CREATE EXTERNAL TABLE IF NOT EXISTS {self.database}.{table_name} "
                    f"({cols_sql}) "
                    f"STORED AS PARQUET "
                    f"LOCATION '{s3_location}'"
                )
            elif storage_format == "TEXTFILE":
                if field_delimiter not in (",", "\\t"):
                    raise ValueError(f"Unsupported field delimiter: {field_delimiter}")
                ddl_sql = (
                    f"CREATE EXTERNAL TABLE IF NOT EXISTS {self.database}.{table_name} "
                    f"({cols_sql}) "
                    f"ROW FORMAT DELIMITED FIELDS TERMINATED BY '{field_delimiter}' "
                    f"STORED AS TEXTFILE "
                    f"LOCATION '{s3_location}' "
                    f"TBLPROPERTIES ('skip.header.line.count'='1')"
                )
            else:
                raise ValueError(f"Unsupported storage format: {storage_format}")

            parsed = sqlglot.parse(ddl_sql, read="hive", error_level=sqlglot.ErrorLevel.RAISE)
            if len(parsed) != 1 or not isinstance(parsed[0], exp.Create):
                raise PermissionError("AST verification failed for constructed CREATE EXTERNAL TABLE DDL.")
        else:
            raise PermissionError(f"Unsupported ingestion DDL operation: {operation}")

        response = self.athena.start_query_execution(
            QueryString=ddl_sql,
            QueryExecutionContext={"Database": self.database},
            ResultConfiguration={"OutputLocation": self.output_location},
            WorkGroup=self.work_group,
        )
        query_execution_id = response["QueryExecutionId"]
        self._poll_athena_execution(query_execution_id)
        return f"Athena Ingestion DDL ({operation.upper()}) Executed: {query_execution_id}"

    def _get_existing_table_s3_prefix(self, table_name: str) -> Optional[str]:
        """Returns the current S3 prefix of an existing Glue table if registered."""
        try:
            resp = self.glue.get_table(DatabaseName=self.database, Name=table_name)
            location = resp.get("Table", {}).get("StorageDescriptor", {}).get("Location", "")
            expected_root = f"s3://{self.bucket_name}/data/{table_name}/"
            if location.startswith(expected_root):
                return location.replace(f"s3://{self.bucket_name}/", "", 1)
        except ClientError:
            pass
        return None

    def _delete_s3_prefix(self, s3_prefix: str) -> None:
        """Deletes objects under a retired S3 ingestion prefix after table swap succeeds."""
        if not s3_prefix or not s3_prefix.startswith("data/"):
            return
        paginator = self.s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket_name, Prefix=s3_prefix):
            contents = page.get("Contents", [])
            if contents:
                delete_keys = [{"Key": obj["Key"]} for obj in contents]
                self.s3.delete_objects(
                    Bucket=self.bucket_name,
                    Delete={"Objects": delete_keys, "Quiet": True},
                )

    def get_schema(self) -> str:
        """Retrieves table and column metadata from the Glue Catalog for prompting."""
        tables_resp = self.glue.get_tables(DatabaseName=self.database)
        schema_lines = []

        for tbl in tables_resp.get("TableList", []):
            tbl_name = tbl["Name"]
            cols = [
                f"{c['Name']} {c['Type']}"
                for c in tbl.get("StorageDescriptor", {}).get("Columns", [])
            ]
            schema_lines.append(
                f"CREATE EXTERNAL TABLE {tbl_name} (\n  "
                + ",\n  ".join(cols)
                + "\n);"
            )

        return (
            "\n\n".join(schema_lines)
            if schema_lines
            else "-- No tables found in database."
        )

    @staticmethod
    def _sanitize_column_names(raw_columns: List[str]) -> List[str]:
        """Sanitizes column identifiers for Glue/Athena and deduplicates collisions."""
        seen: Dict[str, int] = {}
        clean_cols: List[str] = []
        for col in raw_columns:
            base = re.sub(r"[^a-zA-Z0-9_]", "_", str(col).strip().lower()).strip("_")
            if not base or base[0].isdigit():
                base = f"col_{base}" if base else "col_unnamed"
            if base in seen:
                seen[base] += 1
                unique_col = f"{base}_{seen[base]}"
            else:
                seen[base] = 1
                unique_col = base
            clean_cols.append(unique_col)
        return clean_cols

    @classmethod
    def _infer_conservative_csv_columns(
        cls, header: List[str], rows: List[List[str]]
    ) -> List[Tuple[str, str]]:
        """
        Conservatively infers Athena column types from parsed CSV records:
        1. Applies authoritative CUR_COLUMN_TYPE_OVERRIDES for known AWS billing fields.
        2. Only assigns BIGINT or DOUBLE if 100% of non-empty values in the sample parse
           cleanly without mixed text; otherwise defaults to STRING.
        """
        clean_names = cls._sanitize_column_names(header)
        int_pattern = re.compile(r"^-?\d+$")
        float_pattern = re.compile(r"^-?(?:\d+\.\d*|\.\d+|\d+(?:[eE][-+]?\d+))$")

        col_defs: List[Tuple[str, str]] = []
        for col_idx, clean_col in enumerate(clean_names):
            if clean_col in CUR_COLUMN_TYPE_OVERRIDES:
                col_defs.append((clean_col, CUR_COLUMN_TYPE_OVERRIDES[clean_col]))
                continue

            non_empty_vals = [
                row[col_idx].strip()
                for row in rows
                if col_idx < len(row) and row[col_idx] is not None and row[col_idx].strip() != ""
            ]

            # Default empty or tiny samples to STRING for safety
            if len(non_empty_vals) < 5:
                col_defs.append((clean_col, "STRING"))
                continue

            # Keep ID/code/account/zip columns as STRING to preserve leading zeros
            if any(token in clean_col for token in ("id", "code", "arn", "account", "date", "time", "tag")):
                col_defs.append((clean_col, "STRING"))
                continue

            all_ints = all(int_pattern.match(v) for v in non_empty_vals)
            if all_ints:
                col_defs.append((clean_col, "BIGINT"))
                continue

            all_numeric = all(
                int_pattern.match(v) or float_pattern.match(v) for v in non_empty_vals
            )
            if all_numeric:
                col_defs.append((clean_col, "DOUBLE"))
                continue

            col_defs.append((clean_col, "STRING"))

        return col_defs

    @staticmethod
    def _map_arrow_type_to_athena(arrow_type: pa.DataType, field_name: str = "") -> str:
        """
        Maps supported PyArrow primitive types to Athena DDL types.
        Explicitly rejects nested types (struct, list, map) and uint64 to prevent
        silent schema mismatches or integer overflow in Athena.
        """
        if pa.types.is_nested(arrow_type):
            raise ValueError(
                f"Unsupported nested Parquet type '{arrow_type}' on column '{field_name}'. "
                "Please flatten nested structs/lists before ingestion."
            )
        if pa.types.is_uint64(arrow_type):
            raise ValueError(
                f"Unsupported unsigned 64-bit integer (uint64) on column '{field_name}': "
                "Athena BIGINT is signed and may overflow. Cast to DECIMAL(20,0) or INT64 first."
            )
        if pa.types.is_boolean(arrow_type):
            return "BOOLEAN"
        if pa.types.is_int64(arrow_type) or pa.types.is_uint32(arrow_type):
            return "BIGINT"
        if pa.types.is_signed_integer(arrow_type) or pa.types.is_uint8(arrow_type) or pa.types.is_uint16(arrow_type):
            return "INT"
        if pa.types.is_float32(arrow_type):
            return "FLOAT"
        if pa.types.is_float64(arrow_type):
            return "DOUBLE"
        if pa.types.is_decimal(arrow_type):
            return f"DECIMAL({arrow_type.precision},{arrow_type.scale})"
        if pa.types.is_date(arrow_type):
            return "DATE"
        if pa.types.is_timestamp(arrow_type):
            return "TIMESTAMP"
        if pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
            return "STRING"

        raise ValueError(
            f"Unsupported Parquet Arrow data type '{arrow_type}' on column '{field_name}'."
        )

    def stream_upload_and_create_table(self, uploaded_file) -> str:
        """
        Ingests datasets to S3 under a unique versioned prefix (data/<table>/ingestion_id=<uuid>/)
        and registers external tables in the AWS Glue Data Catalog:
        - CSV, TSV, and Parquet files are multipart-streamed in 8 MB chunks.
        - Excel (.xlsx/.xls) files up to 50 MB are converted in-memory to CSV before upload.
        - Previous S3 table prefixes are cleaned up only after the new table registration succeeds.
        """
        fname = uploaded_file.name
        tbl_name = re.sub(r"[^a-zA-Z0-9_]", "_", fname.rsplit(".", 1)[0].lower()).strip("_")
        if not tbl_name or tbl_name[0].isdigit():
            tbl_name = f"tbl_{tbl_name}"

        # Stage into an isolated, unique ingestion prefix so existing data is untouched until cutover
        ingestion_id = uuid.uuid4().hex
        s3_prefix = f"data/{tbl_name}/ingestion_id={ingestion_id}/"
        s3_location = f"s3://{self.bucket_name}/{s3_prefix}"
        previous_s3_prefix = self._get_existing_table_s3_prefix(tbl_name)

        if fname.endswith((".csv", ".tsv")):
            delimiter = "\t" if fname.endswith(".tsv") else ","

            # 1. Parse up to 1,000 complete CSV records using csv.reader (handles embedded newlines in quotes)
            uploaded_file.seek(0)
            text_wrapper = io.TextIOWrapper(uploaded_file, encoding="utf-8", errors="replace", newline="")
            try:
                reader = csv.reader(text_wrapper, delimiter=delimiter)
                header = next(reader, None)
                if not header:
                    raise ValueError(f"Uploaded CSV/TSV file '{fname}' is empty or missing a header row.")
                sample_rows = []
                for _ in range(1000):
                    row = next(reader, None)
                    if row is None:
                        break
                    sample_rows.append(row)
            finally:
                text_wrapper.detach()

            col_defs = self._infer_conservative_csv_columns(header, sample_rows)

            # 2. Rewind stream and multipart-upload directly to the new S3 ingestion prefix
            uploaded_file.seek(0)
            s3_key = f"{s3_prefix}{fname}"
            self.s3.upload_fileobj(
                uploaded_file,
                self.bucket_name,
                s3_key,
                Config=self.transfer_config,
            )

            storage_format = "TEXTFILE"
            field_delimiter = "\\t" if delimiter == "\t" else ","

        elif fname.endswith(".parquet"):
            uploaded_file.seek(0)
            parquet_file = pq.ParquetFile(uploaded_file)
            arrow_schema = parquet_file.schema_arrow
            raw_names = list(arrow_schema.names)
            clean_names = self._sanitize_column_names(raw_names)

            col_defs = []
            for field, clean_col in zip(arrow_schema, clean_names):
                if clean_col in CUR_COLUMN_TYPE_OVERRIDES and pa.types.is_floating(field.type):
                    athena_type = CUR_COLUMN_TYPE_OVERRIDES[clean_col]
                else:
                    athena_type = self._map_arrow_type_to_athena(field.type, field_name=field.name)
                col_defs.append((clean_col, athena_type))

            s3_key = f"{s3_prefix}{fname}"

            if raw_names == clean_names:
                uploaded_file.seek(0)
                self.s3.upload_fileobj(
                    uploaded_file,
                    self.bucket_name,
                    s3_key,
                    Config=self.transfer_config,
                )
            else:
                renamed_fields = [
                    field.with_name(clean_col)
                    for field, clean_col in zip(arrow_schema, clean_names)
                ]
                renamed_schema = pa.schema(renamed_fields)
                with tempfile.NamedTemporaryFile(suffix=".parquet") as tmp_pq:
                    with pq.ParquetWriter(tmp_pq.name, renamed_schema) as writer:
                        for batch in parquet_file.iter_batches(batch_size=50_000):
                            renamed_batch = pa.RecordBatch.from_arrays(
                                batch.columns, schema=renamed_schema
                            )
                            writer.write_batch(renamed_batch)
                    tmp_pq.seek(0)
                    self.s3.upload_fileobj(
                        tmp_pq,
                        self.bucket_name,
                        s3_key,
                        Config=self.transfer_config,
                    )

            storage_format = "PARQUET"
            field_delimiter = ","

        elif fname.endswith((".xlsx", ".xls")):
            # Explicit memory bound for in-memory Excel decompression and CSV conversion
            uploaded_file.seek(0, io.SEEK_END)
            file_size = uploaded_file.tell()
            uploaded_file.seek(0)
            if file_size > MAX_EXCEL_UPLOAD_BYTES:
                raise ValueError(
                    f"Excel file '{fname}' ({file_size / (1024 * 1024):.1f} MB) exceeds the "
                    f"{MAX_EXCEL_UPLOAD_BYTES // (1024 * 1024)} MB limit for in-memory Excel conversion. "
                    "Please convert large workbooks to CSV or Parquet for streaming upload."
                )

            excel_df = pd.read_excel(uploaded_file, dtype=str)
            if excel_df.empty and len(excel_df.columns) == 0:
                raise ValueError(f"Uploaded Excel file '{fname}' contains no columns.")

            header = [str(c) for c in excel_df.columns]
            sample_rows = excel_df.head(1000).fillna("").values.tolist()
            col_defs = self._infer_conservative_csv_columns(header, sample_rows)

            csv_buffer = io.BytesIO()
            excel_df.to_csv(csv_buffer, index=False)
            csv_buffer.seek(0)

            s3_key = f"{s3_prefix}{tbl_name}.csv"
            self.s3.upload_fileobj(
                csv_buffer,
                self.bucket_name,
                s3_key,
                Config=self.transfer_config,
            )

            storage_format = "TEXTFILE"
            field_delimiter = ","
        else:
            raise ValueError(f"Unsupported file format: {fname}")

        # Swap table registration to the newly uploaded S3 prefix
        self._execute_ingestion_ddl(operation="drop", table_name=tbl_name)
        self._execute_ingestion_ddl(
            operation="create",
            table_name=tbl_name,
            col_defs=col_defs,
            storage_format=storage_format,
            s3_location=s3_location,
            field_delimiter=field_delimiter,
        )

        # Clean up retired S3 prefix only after new table cutover succeeds
        if previous_s3_prefix and previous_s3_prefix != s3_prefix:
            try:
                self._delete_s3_prefix(previous_s3_prefix)
            except Exception as cleanup_err:
                logger.warning(f"Non-fatal: could not remove retired S3 prefix '{previous_s3_prefix}': {cleanup_err}")

        return (
            f"Dataset `{fname}` successfully ingested to `{s3_location}` and registered as table"
            f" `{tbl_name}` in Glue Catalog!"
        )
