import io
import logging
import re
import time
import boto3
from boto3.s3.transfer import TransferConfig
import pandas as pd
import pyarrow.parquet as pq
from botocore.exceptions import ClientError

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
        strict_workgroup_check: bool = False,
    ):
        self.database = database
        self.bucket_name = bucket_name
        self.region = region
        self.work_group = work_group
        self.max_scan_mb = max_scan_mb
        self.max_scan_bytes = max_scan_mb * 1024 * 1024
        self.query_timeout_sec = query_timeout_sec
        self.output_location = f"s3://{bucket_name}/athena-results/"
        self.workgroup_guardrail_verified = False

        self.athena = boto3.client("athena", region_name=region)
        self.s3 = boto3.client("s3", region_name=region)
        self.glue = boto3.client("glue", region_name=region)
        self.dialect = "athena"

        # Multipart streaming configuration (8 MB chunks) so large uploads never load fully into RAM
        self.transfer_config = TransferConfig(
            multipart_threshold=8 * 1024 * 1024,
            multipart_chunksize=8 * 1024 * 1024,
            max_concurrency=4,
            use_threads=True,
        )

        # Configure and verify AWS-native 100 MB per-query data scan cutoff on the Athena Workgroup
        self._enforce_workgroup_guardrails(strict=strict_workgroup_check)

    def _enforce_workgroup_guardrails(self, strict: bool = False) -> bool:
        """
        Configures and verifies BytesScannedCutoffPerQuery (default 100 MB) and
        EnforceWorkGroupConfiguration=True on the target Athena Workgroup.
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
                    f"Athena Workgroup '{self.work_group}' cutoff verification mismatch: "
                    f"expected {self.max_scan_bytes} bytes (enforced=True), got {actual_cutoff} (enforced={enforced})."
                )
                logger.warning(msg)
                if strict:
                    raise RuntimeError(msg)
            return self.workgroup_guardrail_verified
        except ClientError as e:
            msg = (
                f"Could not enforce or verify Athena Workgroup '{self.work_group}' "
                f"scan limit ({self.max_scan_mb} MB): {e}"
            )
            logger.warning(msg)
            if strict:
                raise RuntimeError(msg) from e
            return False

    def execute_query(
        self, sql: str, check_cost: bool = False, *args, **kwargs
    ) -> tuple[pd.DataFrame, str]:
        """Executes SQL in Athena within Workgroup scan and timeout limits.

        Handles DDL (CREATE/DROP) and DML (SELECT) properly without NoSuchKey
        errors. Returns (df, query_plan) for full compatibility with agent.py.
        """
        sql_clean = sql.strip().rstrip(";")
        is_ddl_query = sql_clean.upper().startswith((
            "CREATE",
            "DROP",
            "ALTER",
            "MSCK",
            "SHOW",
            "DESCRIBE",
        ))

        response = self.athena.start_query_execution(
            QueryString=sql_clean,
            QueryExecutionContext={"Database": self.database},
            ResultConfiguration={"OutputLocation": self.output_location},
            WorkGroup=self.work_group,
        )
        query_execution_id = response["QueryExecutionId"]
        start_time = time.time()

        # Poll execution status until completion or timeout
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
                break
            elif state in ["FAILED", "CANCELLED"]:
                reason = query_info.get("Status", {}).get(
                    "StateChangeReason", "Unknown error"
                )
                raise RuntimeError(f"Athena Query {state}: {reason}")
            time.sleep(0.5)

        # 1. If it was a DDL statement, Athena does not produce a CSV result
        statement_type = query_info.get("StatementType", "")
        if is_ddl_query or statement_type == "DDL":
            return pd.DataFrame(), f"Athena DDL Executed: {query_execution_id}"

        # 2. Extract the actual output location staged by Athena
        output_location = query_info.get("ResultConfiguration", {}).get(
            "OutputLocation", ""
        )
        if not output_location.endswith(".csv"):
            return pd.DataFrame(), f"Athena Execution ID: {query_execution_id}"

        # Parse bucket and key from output_location (e.g. s3://bucket/athena-results/xyz.csv)
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
    def _build_column_defs_from_df(sample_df: pd.DataFrame, type_map: dict) -> list[str]:
        col_defs = []
        for col, dtype in sample_df.dtypes.items():
            clean_col = re.sub(r"[^a-zA-Z0-9_]", "_", str(col).strip().lower())
            if not clean_col or clean_col[0].isdigit():
                clean_col = f"col_{clean_col}"

            if str(dtype) == "float64" and sample_df[col].isna().all():
                athena_type = "STRING"
            else:
                athena_type = type_map.get(str(dtype), "STRING")

            col_defs.append(f"`{clean_col}` {athena_type}")
        return col_defs

    def stream_upload_and_create_table(self, uploaded_file) -> str:
        """
        Streams CSV, TSV, and PARQUET directly from the file-like stream to S3
        in 8 MB multipart chunks without calling getvalue() (avoiding full-file RAM buffering),
        and registers external tables in the AWS Glue Data Catalog.
        """
        fname = uploaded_file.name
        tbl_name = re.sub(r"[^a-zA-Z0-9_]", "_", fname.rsplit(".", 1)[0].lower())
        s3_prefix = f"data/{tbl_name}/"

        type_map = {
            "int64": "BIGINT",
            "int32": "INT",
            "float64": "DOUBLE",
            "float32": "FLOAT",
            "bool": "BOOLEAN",
            "datetime64[ns]": "TIMESTAMP",
        }

        if fname.endswith((".csv", ".tsv")):
            delimiter = "\t" if fname.endswith(".tsv") else ","

            # 1. Read only a small 256 KB header sample from the stream to infer column types
            uploaded_file.seek(0)
            header_sample_bytes = uploaded_file.read(256 * 1024)
            sample_df = pd.read_csv(
                io.BytesIO(header_sample_bytes), sep=delimiter, nrows=50
            )
            col_defs = self._build_column_defs_from_df(sample_df, type_map)

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
            # 1. Inspect Parquet footer schema metadata without loading full row groups into RAM
            uploaded_file.seek(0)
            parquet_schema = pq.read_schema(uploaded_file)
            arrow_type_map = {
                "int64": "BIGINT",
                "int32": "INT",
                "double": "DOUBLE",
                "float": "FLOAT",
                "bool": "BOOLEAN",
                "timestamp[ns]": "TIMESTAMP",
                "timestamp[us]": "TIMESTAMP",
                "timestamp[ms]": "TIMESTAMP",
            }
            col_defs = []
            for field in parquet_schema:
                clean_col = re.sub(r"[^a-zA-Z0-9_]", "_", str(field.name).strip().lower())
                if not clean_col or clean_col[0].isdigit():
                    clean_col = f"col_{clean_col}"
                athena_type = arrow_type_map.get(str(field.type), "STRING")
                col_defs.append(f"`{clean_col}` {athena_type}")

            # 2. Rewind stream pointer and multipart-stream directly to S3 in 8 MB chunks
            uploaded_file.seek(0)
            s3_key = f"{s3_prefix}{fname}"
            self.s3.upload_fileobj(
                uploaded_file,
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
            # Note: Excel workbooks require in-memory decompression for CSV conversion
            uploaded_file.seek(0)
            excel_df = pd.read_excel(uploaded_file)
            sample_df = excel_df.head(100)
            col_defs = self._build_column_defs_from_df(sample_df, type_map)

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

        # Drop existing table if previously registered, then recreate
        try:
            self.execute_query(
                f"DROP TABLE IF EXISTS {self.database}.{tbl_name}"
            )
        except Exception:
            pass

        self.execute_query(create_sql)
        return (
            f"Dataset `{fname}` successfully ingested to S3 and registered as table"
            f" `{tbl_name}` in Glue Catalog!"
        )
