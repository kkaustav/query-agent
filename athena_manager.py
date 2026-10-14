import io
import itertools
import logging
import re
import tempfile
import time
from typing import List, Tuple
import boto3
from boto3.s3.transfer import TransferConfig
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.exceptions import ClientError

from database import validate_sql_query

logger = logging.getLogger(__name__)


class AthenaManager:

    def __init__(
        self,
        database: str,
        bucket_name: str,
        region: str = "us-east-1",
        work_group: str = "primary",
        max_scan_mb: int = 100,
        query_timeout_sec: int = 30,
        strict_workgroup_check: bool = True,
    ):
        self.database = database
        self.bucket_name = bucket_name
        self.region = region
        self.work_group = work_group
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

        # Multipart streaming configuration (8 MB chunks) so large uploads never load fully into RAM
        self.transfer_config = TransferConfig(
            multipart_threshold=8 * 1024 * 1024,
            multipart_chunksize=8 * 1024 * 1024,
            max_concurrency=4,
            use_threads=True,
        )

        # Configure and verify AWS-native 100 MB per-query data scan cutoff (fail-closed by default)
        self._enforce_workgroup_guardrails(strict=self.strict_workgroup_check)

    def _enforce_workgroup_guardrails(self, strict: bool = True) -> bool:
        """
        Configures and verifies BytesScannedCutoffPerQuery (100 MB) and
        EnforceWorkGroupConfiguration=True on the target Athena Workgroup.
        Fails closed in production when strict=True.
        """
        try:
            self.athena.update_work_group(
                WorkGroup=self.work_group,
                ConfigurationUpdates={
                    "EnforceWorkGroupConfiguration": True,
                    "BytesScannedCutoffPerQuery": self.max_scan_bytes,
                },
            )
            wg_resp = self.athena.get_work_group(WorkGroup=self.work_group)
            wg_config = wg_resp.get("WorkGroup", {}).get("Configuration", {})
            actual_cutoff = wg_config.get("BytesScannedCutoffPerQuery")
            enforced = wg_config.get("EnforceWorkGroupConfiguration", False)

            self.workgroup_guardrail_verified = (
                actual_cutoff == self.max_scan_bytes and enforced is True
            )
            if not self.workgroup_guardrail_verified:
                msg = (
                    f"Fail-Closed FinOps Guardrail Error: Athena Workgroup '{self.work_group}' "
                    f"cutoff verification failed. Expected {self.max_scan_bytes} bytes "
                    f"(enforced=True), got {actual_cutoff} (enforced={enforced})."
                )
                logger.error(msg)
                if strict:
                    raise RuntimeError(msg)
            return self.workgroup_guardrail_verified
        except ClientError as e:
            msg = (
                f"Fail-Closed FinOps Guardrail Error: Could not enforce or verify Athena Workgroup "
                f"'{self.work_group}' scan limit ({self.max_scan_mb} MB): {e}"
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
                raise RuntimeError(f"Athena Query {state}: {reason}")
            time.sleep(0.5)

    def execute_query(
        self, sql: str, check_cost: bool = True, *args, **kwargs
    ) -> Tuple[pd.DataFrame, str]:
        """
        Strictly read-only analytical query execution boundary for Amazon Athena.
        1. Enforces fail-closed Workgroup verification.
        2. Validates the SQL AST via validate_sql_query (raising PermissionError on
           any DDL/DML/multi-statement and ValueError on SELECT *).
        3. Executes the validated SELECT query in Athena and returns (df, plan_summary).
        """
        if self.strict_workgroup_check and not self.workgroup_guardrail_verified:
            raise RuntimeError(
                f"Fail-Closed FinOps Guardrail: Refusing to execute Athena query because "
                f"Workgroup '{self.work_group}' {self.max_scan_mb} MB scan cutoff is not verified."
            )

        # Enforce AST read-only & wildcard guardrails directly inside AthenaManager
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

    def _execute_ingestion_ddl(self, ddl_sql: str, expected_table: str) -> str:
        """
        Privileged internal helper exclusively for schema registration during file ingestion.
        Separated from execute_query() so user/LLM queries can never invoke DDL.
        """
        if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", expected_table):
            raise ValueError(f"Invalid table identifier for ingestion DDL: {expected_table}")

        ddl_clean = ddl_sql.strip().rstrip(";")
        allowed_prefixes = (
            f"DROP TABLE IF EXISTS {self.database}.{expected_table}".upper(),
            f"CREATE EXTERNAL TABLE IF NOT EXISTS {self.database}.{expected_table}".upper(),
        )
        normalized_upper = " ".join(ddl_clean.upper().split())
        if not normalized_upper.startswith(allowed_prefixes):
            raise PermissionError(
                "Internal DDL boundary rejected unexpected statement outside table registration scope."
            )

        response = self.athena.start_query_execution(
            QueryString=ddl_clean,
            QueryExecutionContext={"Database": self.database},
            ResultConfiguration={"OutputLocation": self.output_location},
            WorkGroup=self.work_group,
        )
        query_execution_id = response["QueryExecutionId"]
        self._poll_athena_execution(query_execution_id)
        return f"Athena Ingestion DDL Executed: {query_execution_id}"

    def _clear_s3_prefix(self, s3_prefix: str) -> None:
        """
        Deletes any existing S3 objects under data/<table_name>/ prior to uploading
        a new dataset so Athena never scans stale or conflicting files in the table prefix.
        """
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
        seen = {}
        clean_cols = []
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
    def _build_column_defs_from_df(cls, sample_df: pd.DataFrame) -> List[str]:
        """Infers resilient Athena column types from a sample DataFrame."""
        clean_names = cls._sanitize_column_names(list(sample_df.columns))
        col_defs = []

        for raw_col, clean_col in zip(sample_df.columns, clean_names):
            series = sample_df[raw_col]
            non_null = series.dropna()

            if non_null.empty:
                athena_type = "STRING"
            elif pd.api.types.is_bool_dtype(series):
                athena_type = "BOOLEAN"
            elif pd.api.types.is_integer_dtype(series):
                # Promote integers to BIGINT so larger values later in the file never overflow
                athena_type = "BIGINT"
            elif pd.api.types.is_float_dtype(series):
                athena_type = "DOUBLE"
            elif pd.api.types.is_datetime64_any_dtype(series):
                athena_type = "TIMESTAMP"
            else:
                athena_type = "STRING"

            col_defs.append(f"`{clean_col}` {athena_type}")
        return col_defs

    @staticmethod
    def _map_arrow_type_to_athena(arrow_type: pa.DataType) -> str:
        """Maps PyArrow data types accurately to Athena/Hive DDL types."""
        if pa.types.is_boolean(arrow_type):
            return "BOOLEAN"
        if pa.types.is_int64(arrow_type) or pa.types.is_uint64(arrow_type):
            return "BIGINT"
        if pa.types.is_integer(arrow_type):
            return "INT"
        if pa.types.is_float32(arrow_type):
            return "FLOAT"
        if pa.types.is_floating(arrow_type):
            return "DOUBLE"
        if pa.types.is_decimal(arrow_type):
            return f"DECIMAL({arrow_type.precision},{arrow_type.scale})"
        if pa.types.is_date(arrow_type):
            return "DATE"
        if pa.types.is_timestamp(arrow_type):
            return "TIMESTAMP"
        return "STRING"

    def stream_upload_and_create_table(self, uploaded_file) -> str:
        """
        Clears stale S3 objects in the target prefix, streams CSV/TSV/Parquet datasets
        to S3 in 8 MB multipart chunks, and registers external tables via _execute_ingestion_ddl.
        """
        fname = uploaded_file.name
        tbl_name = re.sub(r"[^a-zA-Z0-9_]", "_", fname.rsplit(".", 1)[0].lower()).strip("_")
        if not tbl_name or tbl_name[0].isdigit():
            tbl_name = f"tbl_{tbl_name}"
        s3_prefix = f"data/{tbl_name}/"

        # Remove any stale S3 objects in this table's prefix before uploading new data
        self._clear_s3_prefix(s3_prefix)

        if fname.endswith((".csv", ".tsv")):
            delimiter = "\t" if fname.endswith(".tsv") else ","

            # 1. Read up to 1,000 complete lines safely (never slicing mid-line or mid-quote)
            uploaded_file.seek(0)
            text_wrapper = io.TextIOWrapper(uploaded_file, encoding="utf-8", errors="replace", newline="")
            try:
                sample_lines = list(itertools.islice(text_wrapper, 1001))
            finally:
                # Detach wrapper so underlying uploaded_file stream is not closed
                text_wrapper.detach()

            sample_text = "".join(sample_lines)
            sample_df = pd.read_csv(
                io.StringIO(sample_text), sep=delimiter, low_memory=False
            )
            col_defs = self._build_column_defs_from_df(sample_df)

            # 2. Rewind stream pointer and multipart-stream directly to S3 in 8 MB chunks
            uploaded_file.seek(0)
            s3_key = f"{s3_prefix}{fname}"
            self.s3.upload_fileobj(
                uploaded_file,
                self.bucket_name,
                s3_key,
                Config=self.transfer_config,
            )

            term_char = "\\t" if delimiter == "\t" else ","
            create_sql = f"""
            CREATE EXTERNAL TABLE IF NOT EXISTS {self.database}.{tbl_name} (
                {', '.join(col_defs)}
            )
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY '{term_char}'
            STORED AS TEXTFILE
            LOCATION 's3://{self.bucket_name}/{s3_prefix}'
            TBLPROPERTIES ('skip.header.line.count'='1');
            """

        elif fname.endswith(".parquet"):
            # 1. Inspect Parquet footer schema and sanitize/deduplicate column names
            uploaded_file.seek(0)
            parquet_file = pq.ParquetFile(uploaded_file)
            arrow_schema = parquet_file.schema_arrow
            raw_names = list(arrow_schema.names)
            clean_names = self._sanitize_column_names(raw_names)

            col_defs = []
            for field, clean_col in zip(arrow_schema, clean_names):
                athena_type = self._map_arrow_type_to_athena(field.type)
                col_defs.append(f"`{clean_col}` {athena_type}")

            s3_key = f"{s3_prefix}{fname}"

            # 2. Because Athena maps Parquet columns by field name, if column names were
            # sanitized (e.g., spaces -> underscores), rewrite the Parquet schema in streaming batches
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

            create_sql = f"""
            CREATE EXTERNAL TABLE IF NOT EXISTS {self.database}.{tbl_name} (
                {', '.join(col_defs)}
            )
            STORED AS PARQUET
            LOCATION 's3://{self.bucket_name}/{s3_prefix}';
            """

        elif fname.endswith((".xlsx", ".xls")):
            uploaded_file.seek(0)
            excel_df = pd.read_excel(uploaded_file)
            col_defs = self._build_column_defs_from_df(excel_df.head(1000))

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

            create_sql = f"""
            CREATE EXTERNAL TABLE IF NOT EXISTS {self.database}.{tbl_name} (
                {', '.join(col_defs)}
            )
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            STORED AS TEXTFILE
            LOCATION 's3://{self.bucket_name}/{s3_prefix}'
            TBLPROPERTIES ('skip.header.line.count'='1');
            """
        else:
            raise ValueError(f"Unsupported file format: {fname}")

        # Register schema using the separated internal ingestion DDL runner
        self._execute_ingestion_ddl(
            f"DROP TABLE IF EXISTS {self.database}.{tbl_name}",
            expected_table=tbl_name,
        )
        self._execute_ingestion_ddl(create_sql, expected_table=tbl_name)

        return (
            f"Dataset `{fname}` successfully ingested to S3 and registered as table"
            f" `{tbl_name}` in Glue Catalog!"
        )
