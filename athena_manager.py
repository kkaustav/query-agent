import io
import re
import time
import boto3
import pandas as pd
from botocore.exceptions import ClientError


class AthenaManager:

  def __init__(
      self, database: str, bucket_name: str, region: str = "us-east-1"
  ):
    self.database = database
    self.bucket_name = bucket_name
    self.region = region
    self.output_location = f"s3://{bucket_name}/athena-results/"

    self.athena = boto3.client("athena", region_name=region)
    self.s3 = boto3.client("s3", region_name=region)
    self.glue = boto3.client("glue", region_name=region)
    self.dialect = "athena"

  def execute_query(
      self, sql: str, check_cost: bool = False, *args, **kwargs
  ) -> tuple[pd.DataFrame, str]:
    """Executes SQL in Athena.

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
    )
    query_execution_id = response["QueryExecutionId"]

    # Poll execution status until completion
    while True:
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

    plan_summary = f"Athena Execution ID: {query_execution_id}"
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

  def stream_upload_and_create_table(self, uploaded_file) -> str:
    """Streams CSV, TSV, XLSX, XLS, and PARQUET to S3 and registers external

    tables in Glue.
    """
    fname = uploaded_file.name
    tbl_name = re.sub(r"[^a-zA-Z0-9_]", "_", fname.rsplit(".", 1)[0].lower())
    s3_prefix = f"data/{tbl_name}/"

    # Read bytes safely in memory to prevent closed file stream exceptions
    file_bytes = uploaded_file.getvalue()

    type_map = {
        "int64": "BIGINT",
        "int32": "INT",
        "float64": "DOUBLE",
        "float32": "FLOAT",
        "bool": "BOOLEAN",
        "datetime64[ns]": "TIMESTAMP",
    }

    # Format handling & DDL schema construction
    if fname.endswith((".csv", ".tsv")):
      delimiter = "\t" if fname.endswith(".tsv") else ","
      sample_df = pd.read_csv(
          io.BytesIO(file_bytes), sep=delimiter, nrows=100
      )

      # Upload raw file directly into its dedicated S3 table prefix
      s3_key = f"{s3_prefix}{fname}"
      self.s3.upload_fileobj(
          io.BytesIO(file_bytes), self.bucket_name, s3_key
      )

      col_defs = []
      for col, dtype in sample_df.dtypes.items():
        clean_col = re.sub(r"[^a-zA-Z0-9_]", "_", str(col).strip().lower())
        if not clean_col or clean_col[0].isdigit():
          clean_col = f"col_{clean_col}"

        # If column contains only nulls, default to STRING
        if str(dtype) == "float64" and sample_df[col].isna().all():
          athena_type = "STRING"
        else:
          athena_type = type_map.get(str(dtype), "STRING")

        col_defs.append(f"`{clean_col}` {athena_type}")

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

    elif fname.endswith((".xlsx", ".xls")):
      excel_df = pd.read_excel(io.BytesIO(file_bytes))
      csv_buffer = io.BytesIO()
      excel_df.to_csv(csv_buffer, index=False)
      csv_bytes = csv_buffer.getvalue()

      # Upload converted CSV to S3
      s3_key = f"{s3_prefix}{tbl_name}.csv"
      self.s3.upload_fileobj(
          io.BytesIO(csv_bytes), self.bucket_name, s3_key
      )

      sample_df = excel_df.head(100)
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

    elif fname.endswith(".parquet"):
      s3_key = f"{s3_prefix}{fname}"
      self.s3.upload_fileobj(
          io.BytesIO(file_bytes), self.bucket_name, s3_key
      )

      sample_df = pd.read_parquet(io.BytesIO(file_bytes))
      col_defs = []
      for col, dtype in sample_df.dtypes.items():
        clean_col = re.sub(r"[^a-zA-Z0-9_]", "_", str(col).strip().lower())
        athena_type = type_map.get(str(dtype), "STRING")
        col_defs.append(f"`{clean_col}` {athena_type}")

      create_sql = f"""
            CREATE EXTERNAL TABLE IF NOT EXISTS {self.database}.{tbl_name} (
                {', '.join(col_defs)}
            )
            STORED AS PARQUET
            LOCATION 's3://{self.bucket_name}/{s3_prefix}';
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