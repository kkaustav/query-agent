import io
import time
import datetime
import boto3
import pandas as pd

# Fetch dynamic AWS environment settings
session = boto3.Session()
sts = session.client("sts")
region = session.region_name or "us-east-1"
account_id = sts.get_caller_identity()["Account"]

BUCKET_NAME = f"query-agent-lake-{account_id}"
DATABASE_NAME = "query_agent_db"

s3 = session.client("s3", region_name=region)
athena = session.client("athena", region_name=region)
glue = session.client("glue", region_name=region)

print(f"⚡ Provisioning Data Lake in S3 bucket: s3://{BUCKET_NAME}")
print(f"⚡ Target AWS Glue Database: {DATABASE_NAME}")

# 1. Ensure S3 Bucket exists
try:
    if region == "us-east-1":
        s3.create_bucket(Bucket=BUCKET_NAME)
    else:
        s3.create_bucket(
            Bucket=BUCKET_NAME,
            CreateBucketConfiguration={"LocationConstraint": region}
        )
    print(f"✅ S3 bucket created: {BUCKET_NAME}")
except Exception as e:
    print(f"ℹ️ S3 Bucket check: {e}")

# 2. Ensure Glue Database exists
try:
    glue.create_database(
        DatabaseInput={
            "Name": DATABASE_NAME,
            "Description": "Data Lake catalog for NL Query Agent"
        }
    )
    print(f"✅ Glue database created: {DATABASE_NAME}")
except Exception as e:
    print(f"ℹ️ Glue Database check: {e}")

# 3. Generate Mock E-commerce Data
customers_df = pd.DataFrame([
    {"customer_id": 1, "name": "Alice Johnson", "email": "alice@example.com", "country": "USA",
     "created_at": "2025-01-10 09:30:00"},
    {"customer_id": 2, "name": "Bob Smith", "email": "bob@example.com", "country": "UK",
     "created_at": "2025-01-12 14:15:00"},
    {"customer_id": 3, "name": "Charlie Lee", "email": "charlie@example.com", "country": "Germany",
     "created_at": "2025-02-01 11:00:00"},
    {"customer_id": 4, "name": "Diana Prince", "email": "diana@example.com", "country": "Canada",
     "created_at": "2025-02-15 16:45:00"},
    {"customer_id": 5, "name": "Evan Wright", "email": "evan@example.com", "country": "Australia",
     "created_at": "2025-03-01 10:20:00"}
])

products_df = pd.DataFrame([
    {"product_id": 101, "product_name": "Mechanical Keyboard", "category": "Electronics", "price": 129.99, "stock": 45},
    {"product_id": 102, "product_name": "Wireless Ergonomic Mouse", "category": "Electronics", "price": 59.99,
     "stock": 120},
    {"product_id": 103, "product_name": "Monitor Light Bar", "category": "Accessories", "price": 49.50, "stock": 80},
    {"product_id": 104, "product_name": "Standing Desk Mat", "category": "Office", "price": 39.00, "stock": 15},
    {"product_id": 105, "product_name": "USB-C Dual Dock", "category": "Electronics", "price": 89.99, "stock": 25}
])

orders_df = pd.DataFrame([
    {"order_id": 1001, "customer_id": 1, "order_date": "2025-03-01 14:22:00", "status": "Completed",
     "total_amount": 189.98},
    {"order_id": 1002, "customer_id": 2, "order_date": "2025-03-02 09:10:00", "status": "Completed",
     "total_amount": 129.99},
    {"order_id": 1003, "customer_id": 1, "order_date": "2025-03-05 18:30:00", "status": "Pending",
     "total_amount": 49.50},
    {"order_id": 1004, "customer_id": 3, "order_date": "2025-03-10 11:15:00", "status": "Completed",
     "total_amount": 179.49},
    {"order_id": 1005, "customer_id": 4, "order_date": "2025-03-12 16:00:00", "status": "Cancelled",
     "total_amount": 39.00},
    {"order_id": 1006, "customer_id": 5, "order_date": "2025-03-15 12:45:00", "status": "Completed",
     "total_amount": 279.48}
])

order_items_df = pd.DataFrame([
    {"item_id": 1, "order_id": 1001, "product_id": 101, "quantity": 1, "unit_price": 129.99},
    {"item_id": 2, "order_id": 1001, "product_id": 102, "quantity": 1, "unit_price": 59.99},
    {"item_id": 3, "order_id": 1002, "product_id": 101, "quantity": 1, "unit_price": 129.99},
    {"item_id": 4, "order_id": 1003, "product_id": 103, "quantity": 1, "unit_price": 49.50},
    {"item_id": 5, "order_id": 1004, "product_id": 101, "quantity": 1, "unit_price": 129.99},
    {"item_id": 6, "order_id": 1004, "product_id": 103, "quantity": 1, "unit_price": 49.50},
    {"item_id": 7, "order_id": 1005, "product_id": 104, "quantity": 1, "unit_price": 39.00},
    {"item_id": 8, "order_id": 1006, "product_id": 101, "quantity": 1, "unit_price": 129.99},
    {"item_id": 9, "order_id": 1006, "product_id": 102, "quantity": 1, "unit_price": 59.99},
    {"item_id": 10, "order_id": 1006, "product_id": 105, "quantity": 1, "unit_price": 89.99}
])

tables = {
    "customers": (customers_df, """
        CREATE EXTERNAL TABLE IF NOT EXISTS query_agent_db.customers (
            customer_id INT,
            name STRING,
            email STRING,
            country STRING,
            created_at STRING
        )
        ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
        STORED AS TEXTFILE
        LOCATION 's3://{bucket}/data/customers/'
        TBLPROPERTIES ('skip.header.line.count'='1');
    """),
    "products": (products_df, """
        CREATE EXTERNAL TABLE IF NOT EXISTS query_agent_db.products (
            product_id INT,
            product_name STRING,
            category STRING,
            price DOUBLE,
            stock INT
        )
        ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
        STORED AS TEXTFILE
        LOCATION 's3://{bucket}/data/products/'
        TBLPROPERTIES ('skip.header.line.count'='1');
    """),
    "orders": (orders_df, """
        CREATE EXTERNAL TABLE IF NOT EXISTS query_agent_db.orders (
            order_id INT,
            customer_id INT,
            order_date STRING,
            status STRING,
            total_amount DOUBLE
        )
        ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
        STORED AS TEXTFILE
        LOCATION 's3://{bucket}/data/orders/'
        TBLPROPERTIES ('skip.header.line.count'='1');
    """),
    "order_items": (order_items_df, """
        CREATE EXTERNAL TABLE IF NOT EXISTS query_agent_db.order_items (
            item_id INT,
            order_id INT,
            product_id INT,
            quantity INT,
            unit_price DOUBLE
        )
        ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
        STORED AS TEXTFILE
        LOCATION 's3://{bucket}/data/order_items/'
        TBLPROPERTIES ('skip.header.line.count'='1');
    """)
}


def execute_athena_ddl(sql: str):
    output_loc = f"s3://{BUCKET_NAME}/athena-results/"
    resp = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": DATABASE_NAME},
        ResultConfiguration={"OutputLocation": output_loc}
    )
    qid = resp["QueryExecutionId"]
    while True:
        status = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]["State"]
        if status in ["SUCCEEDED"]:
            return True
        elif status in ["FAILED", "CANCELLED"]:
            raise RuntimeError(f"Athena DDL failed with status {status}")
        time.sleep(0.5)


# 4. Upload CSV files and create Athena tables
for tbl_name, (df, ddl_template) in tables.items():
    csv_buffer = io.BytesIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)

    s3_key = f"data/{tbl_name}/{tbl_name}.csv"
    s3.put_object(Bucket=BUCKET_NAME, Key=s3_key, Body=csv_buffer.getvalue())
    print(f"📤 Uploaded: s3://{BUCKET_NAME}/{s3_key}")

    formatted_ddl = ddl_template.format(bucket=BUCKET_NAME)
    execute_athena_ddl(formatted_ddl)
    print(f"📊 Registered Athena Table: `{tbl_name}`")

print("\n🚀 All tables seeded and registered successfully in Amazon Athena!")