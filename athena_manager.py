import time
import re
import boto3
import pandas as pd


class AthenaManager:
    def __init__(self, database: str, bucket_name: str, region: str = "us-east-1"):
        self.database = database
        self.bucket_name = bucket_name
        self.region = region
        self.output_location = f"s3://{bucket_name}/athena-results/"

        self.athena = boto3.client("athena", region_name=region)
        self.s3 = boto3.client("s3", region_name=region)
        self.glue = boto3.client("glue", region_name=region)
        self.dialect = "athena"

    def execute_query(self, sql: str, check_cost: bool = False, *args, **kwargs) -> tuple[pd.DataFrame, str]:
        """
        Executes Trino/Presto SQL in Athena. Returns (df, query_plan)
        to match the interface expected by agent.py's self-healing loop.
        """
        sql_clean = sql.strip().rstrip(";")

        response = self.athena.start_query_execution(
            QueryString=sql_clean,
            QueryExecutionContext={"Database": self.database},
            ResultConfiguration={"OutputLocation": self.output_location}
        )
        query_execution_id = response["QueryExecutionId"]

        # Poll query execution status
        while True:
            status_resp = self.athena.get_query_execution(QueryExecutionId=query_execution_id)
            state = status_resp["QueryExecution"]["Status"]["State"]

            if state == "SUCCEEDED":
                break
            elif state in ["FAILED", "CANCELLED"]:
                reason = status_resp["QueryExecution"]["Status"].get("StateChangeReason", "Unknown error")
                raise RuntimeError(f"Athena Query {state}: {reason}")
            time.sleep(0.5)

        # Download result directly from Athena output S3 path
        result_key = f"athena-results/{query_execution_id}.csv"
        obj = self.s3.get_object(Bucket=self.bucket_name, Key=result_key)
        try:
            df = pd.read_csv(obj["Body"])
        except pd.errors.EmptyDataError:
            df = pd.DataFrame()

        plan_summary = f"Athena Execution ID: {query_execution_id}"
        return df, plan_summary

    def get_schema(self) -> str:
        """Retrieves table and column metadata from the Glue Catalog for prompting."""
        tables_resp = self.glue.get_tables(DatabaseName=self.database)
        schema_lines = []

        for tbl in tables_resp.get("TableList", []):
            tbl_name = tbl["Name"]
            cols = [f"{c['Name']} {c['Type']}" for c in tbl.get("StorageDescriptor", {}).get("Columns", [])]
            schema_lines.append(f"CREATE EXTERNAL TABLE {tbl_name} (\n  " + ",\n  ".join(cols) + "\n);")

        return "\n\n".join(schema_lines) if schema_lines else "-- No tables found in database."

    def stream_upload_and_create_table(self, uploaded_file) -> str:
        """
        Streams uploaded file directly to S3 and registers an external table in Athena.
        """
        fname = uploaded_file.name
        tbl_name = re.sub(r'[^a-zA-Z0-9_]', '_', fname.rsplit('.', 1)[0].lower())
        s3_prefix = f"data/{tbl_name}/"
        s3_key = f"{s3_prefix}{fname}"

        self.s3.upload_fileobj(uploaded_file, self.bucket_name, s3_key)

        if fname.endswith(".csv"):
            uploaded_file.seek(0)
            sample_df = pd.read_csv(uploaded_file, nrows=100)

            type_map = {
                "int64": "BIGINT",
                "float64": "DOUBLE",
                "bool": "BOOLEAN",
                "datetime64[ns]": "TIMESTAMP"
            }

            col_defs = []
            for col, dtype in sample_df.dtypes.items():
                clean_col = re.sub(r'[^a-zA-Z0-9_]', '_', str(col).lower())
                athena_type = type_map.get(str(dtype), "STRING")
                col_defs.append(f"`{clean_col}` {athena_type}")

            create_sql = f"""
            CREATE EXTERNAL TABLE IF NOT EXISTS {self.database}.{tbl_name} (
                {', '.join(col_defs)}
            )
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            ESCAPED BY '\\\\'
            LINES TERMINATED BY '\\n'
            LOCATION 's3://{self.bucket_name}/{s3_prefix}'
            TBLPROPERTIES ('skip.header.line.count'='1');
            """

            self.execute_query(create_sql)
            return f"Table `{tbl_name}` created and registered in Glue Catalog!"

        elif fname.endswith(".parquet"):
            create_sql = f"""
            CREATE EXTERNAL TABLE IF NOT EXISTS {self.database}.{tbl_name}
            STORED AS PARQUET
            LOCATION 's3://{self.bucket_name}/{s3_prefix}';
            """
            self.execute_query(create_sql)
            return f"Parquet table `{tbl_name}` registered in Glue Catalog!"

        return f"Uploaded {fname} to S3."