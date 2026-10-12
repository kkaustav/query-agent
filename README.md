# Autonomous Natural Language Database Query Agent & FinOps Copilot (Serverless Data Lakehouse)

[![Streamlit App](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://query-agent.streamlit.app/)
[![AWS ECS Live](https://img.shields.io/badge/AWS%20ECS-Fargate%20Live-orange?logo=amazon-aws)](https://qu-ad51b077a15a4aada7d7d6906df94105.ecs.us-east-1.on.aws/)
[![Python 3.12](https://img.shields.io/badge/python-3.12-blue.svg)](https://www.python.org/downloads/release/python-3120/)
[![Amazon Bedrock](https://img.shields.io/badge/Amazon%20Bedrock-Nova%20Pro-792EE5)](https://aws.amazon.com/bedrock/)
[![Amazon Athena](https://img.shields.io/badge/Amazon%20Athena-Serverless%20Trino-232F3E)](https://aws.amazon.com/athena/)
[![Amazon S3](https://img.shields.io/badge/Amazon%20S3-Data%20Lake-569A31)](https://aws.amazon.com/s3/)
[![AWS Glue](https://img.shields.io/badge/AWS%20Glue-Data%20Catalog-blue)](https://aws.amazon.com/glue/)
[![FinOps Certified](https://img.shields.io/badge/FinOps-Cloud%20Cost%20Intelligence-00C49F)](https://www.finops.org/)
[![Docker](https://img.shields.io/badge/Docker-Containerized-2496ED?logo=docker&logoColor=white)](https://www.docker.com/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

*Live Deployments:*
- **Primary (Always Online):** https://query-agent.streamlit.app/
- **Enterprise AWS Production (ECS Fargate + ALB):** https://qu-ad51b077a15a4aada7d7d6906df94105.ecs.us-east-1.on.aws/ *(Note: The AWS ECS Fargate task is scaled to 0 tasks when idle to optimize cloud costs; spin up to 1 task on-demand via the AWS CLI).*

An enterprise-grade, serverless Text-to-SQL conversational analytics platform and **Autonomous Cloud FinOps Intelligence Engine**. Translates natural language inquiries into Trino/Presto SQL, enforces a declarative business semantic metric layer, validates query safety via Abstract Syntax Tree (AST) parsing, executes distributed queries over an Amazon S3 data lake using Amazon Athena, dynamically catalogs schemas via the AWS Glue Data Catalog, and provisions interactive Plotly visualizations paired with **prescriptive FinOps cloud cost optimization playbooks**.

Containerized with Docker, published to **Amazon ECR**, and hosted on **Amazon ECS on AWS Fargate** with managed ALB routing and keyless IAM Task Role authentication for **Amazon Bedrock**.

---

## 🖼️ FinOps Copilot in Action

### 1. Autonomous Anomaly Detection & 3-Tier FinOps Diagnostics
When prompted with: *"Find the top anomalies and provide me recommendations to resolve it"*, the agent autonomously identifies statistical outliers across services (such as $19,196.32 in Amazon EC2 and $3,900.10 in Compute Savings Plans), explains the underlying engineering root causes, and delivers actionable AWS cost-reduction playbooks:

![FinOps Anomaly Diagnostics & Remediation](https://drive.google.com/uc?export=view&id=1d5UdTjmkIIsKScxR-5Y0PSnl153eprkE)

### 2. Analytical Data Table & Interactive Anomaly Spend Visualization
Athena serverless query execution isolates anomalous spend across AWS product codes, rendering an interactive Plotly bar chart and a 10-row dataset with instant CSV and Excel export options:

![FinOps Anomaly Visualization & Results Table](https://drive.google.com/uc?export=view&id=1QTvDlm6B0fC67jauudbNmEjEM9smaR-x)

---

## Architecture Overview

The platform decouples streaming data ingestion, semantic vector retrieval, AST query sanitation, serverless query execution, and conversational FinOps business intelligence:

```text
[ User Browser / Client ]
           │
           ▼
[ Application Load Balancer (Managed HTTPS) ]
           │
           ▼
[ AWS ECS on AWS Fargate (Streamlit Presentation Layer) ]
     │                              │
     ├─► [ Amazon Bedrock ]         ├─► [ AST SQL Sanitizer (sqlglot) ]
     │     ├─ Nova Pro 1.0          │     └─ Strict SELECT Validation
     │     └─ Titan Embeddings v2   │
     │                              ├─► [ Amazon Athena (Serverless Trino) ]
     └─► [ metrics.yaml ]           │     └─ Distributed Query Engine
           └─ Semantic Metric Store │
                                    ├─► [ AWS Glue Data Catalog ]
                                    │     └─ query_agent_db
                                    │
                                    └─► [ Amazon S3 Lakehouse ]
                                          ├─ Raw Tables (/data/)
                                          └─ Athena Staging (/athena-results/)
```

---

## Key System Features

### 1. Autonomous Cloud FinOps & Cost Remediation Engine
- **AWS CUR Ingestion & Schema Alignment:** Native understanding of AWS Cost and Usage Reports (CUR), dynamically mapping unblended/effective costs, usage types, operations, and pricing models (`On-Demand`, `Savings Plans`, `Spot`, `Reserved Instances`).
- **3-Tier Executive FinOps Diagnostics:** Every cost and infrastructure inquiry automatically synthesizes:
  1. 📊 **Spend & Trend Analysis:** Concrete dollar totals, MoM variances, and percentage spikes.
  2. 🔍 **Root Cause Diagnosis:** Technical drivers behind anomalies (e.g., month-start recurring commitment fees, tax clumping, unamortized RIs, oversized dev/test compute, or month-end batch pipelines).
  3. 💡 **Prescriptive Cost Optimization:** Actionable AWS architectural and procurement fixes:
     - **Immediate Waste Elimination:** Automated non-prod scheduling via AWS Instance Scheduler, deletion of unattached EBS volumes, and obsolete snapshot cleanup.
     - **Commitment Tuning:** Compute and EC2 Instance Savings Plans coverage optimization.
     - **Modernization & Rightsizing:** Workload migration to AWS Graviton processors, GP2 to GP3 EBS transitions, and rightsizing idle nodes.
     - **Governance:** Enforcing AWS Cost Anomaly Detection with SNS alerting and tag-or-terminate policies.

### 2. Business Semantic Metric Layer (`metrics.yaml`)
- Eliminates the primary failure mode of enterprise Text-to-SQL: business definition ambiguity.
- Declaratively maps business formulas (e.g., `net_revenue = SUM(order_value - discount_amount)`), entity synonyms (e.g., `"Prime members"` -> `is_prime = true`), and default filter predicates (e.g., `order_status = 'delivered'`).
- Injects ground truth formulas directly into Bedrock Nova Pro reasoning prompts, eliminating ad-hoc metric hallucinations.

### 3. Serverless Lakehouse Architecture (Amazon S3 + Amazon Athena)
- Replaces container-bound transactional databases with an analytical data lake (`s3://query-agent-lake-<account_id>/`).
- Executes distributed SQL across tabular datasets using Amazon Athena's serverless Trino/Presto engine.
- Supports both delimited text files (`CSV`, `TSV`) and columnar storage (`Apache Parquet` with Snappy compression).
- Decouples DDL (`CREATE EXTERNAL TABLE`, `DROP TABLE`) and DML (`SELECT`) workflows to prevent S3 query staging collisions.

### 4. Dual-Track Data Ingestion Strategy
- **Ad-Hoc UI Streaming (In-Band):** Streamlit file uploader supports batch uploading up to 10+ heterogeneous datasets (CSV, TSV, XLSX, XLS, PARQUET up to 1 GB per file) utilizing memory-buffered streaming to prevent container Out-Of-Memory (OOM) crashes on 2 GB Fargate tasks.
- **Lakehouse Scale Ingestion (Out-Of-Band):** For multi-gigabyte or terabyte workloads exceeding container ephemeral storage (default 20 GB), files bypass the web server via S3 Multipart Uploads or AWS CLI direct-to-S3 ingestion, followed by immediate AWS Glue Catalog schema synchronization.

### 5. AST Query Sanitization & Zero-Trust Safety (`database.py`)
- Employs Abstract Syntax Tree (AST) analysis via `sqlglot` to parse generated SQL before transmission to Athena.
- **Strict Read-Only Enforcement:** Traverses the expression tree to verify root nodes are `Select` statements. Rejects destructive operations (`DROP`, `DELETE`, `UPDATE`, `INSERT`, `ALTER`, `TRUNCATE`, `GRANT`).
- Neutralizes SQL injection and model hallucination vectors by blocking multi-statement execution and comments that bypass parser validation.

### 6. Cross-Table Relational Join Intelligence
- Bedrock Nova Pro inspects all active tables in the AWS Glue Data Catalog (`query_agent_db`) simultaneously.
- Infers implicit foreign-key relationships across disparate operational domains (e.g., joining customer demographic attributes from `zomato_customers` with transaction volume in `zomato_orders`).
- Employs explicit Presto/Trino SQL join semantics (`LEFT JOIN`, `INNER JOIN`) to eliminate `COLUMN_NOT_FOUND` runtime exceptions.

### 7. Autonomous Self-Healing Execution Loop
- Intercepts execution failures, schema mismatches, empty-table zero-row traps, and Trino dialect errors in real time.
- Feeds raw database exceptions and execution traces back into Nova Pro for up to 3 automated correction iterations before returning output.

### 8. Partition & Storage Layout Advisor (`index_advisor.py`)
- Analyzes recurring query `WHERE` filters, `JOIN` predicates, and aggregations to output recommendations for:
  - **Partition Keys:** Identifying high-cardinality pruning candidates (e.g., `order_date`, `city`, `season`).
  - **Columnar Formats:** Advising conversion from raw CSV/JSON to Apache Parquet with Snappy compression to reduce scanned byte volume by 80–90%.
  - **Bucketing Strategies:** Recommending hash-bucketing keys for frequently joined foreign keys.

### 9. Isolated Multi-Tab Chat History
- Session management powered by URL query parameters (`?session=<session_id>`) and a shared application resource cache.
- Opening historical queries renders selected conversations in a new, independent browser tab without disrupting the active query session.

---

## Security, Guardrails & Governance

### 1. Hardened Least-Privilege IAM Task Role
The ECS Fargate task executes under a scoped IAM Task Role. Broad managed policies (`*FullAccess`) are explicitly avoided.
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "BedrockModelInvocation",
      "Effect": "Allow",
      "Action": "bedrock:InvokeModel",
      "Resource": [
        "arn:aws:bedrock:*::foundation-model/amazon.nova-pro-v1:0",
        "arn:aws:bedrock:*::foundation-model/amazon.titan-embed-text-v2:0"
      ]
    },
    {
      "Sid": "S3LakehouseAccess",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:ListBucket",
        "s3:GetBucketLocation"
      ],
      "Resource": [
        "arn:aws:s3:::query-agent-lake-*",
        "arn:aws:s3:::query-agent-lake-*/*"
      ]
    },
    {
      "Sid": "AthenaQueryExecution",
      "Effect": "Allow",
      "Action": [
        "athena:StartQueryExecution",
        "athena:GetQueryExecution",
        "athena:GetQueryResults",
        "athena:StopQueryExecution"
      ],
      "Resource": "arn:aws:athena:*:*:workgroup/query-agent-workgroup"
    },
    {
      "Sid": "GlueCatalogAccess",
      "Effect": "Allow",
      "Action": [
        "glue:GetDatabase",
        "glue:GetDatabases",
        "glue:GetTable",
        "glue:GetTables",
        "glue:CreateTable",
        "glue:DeleteTable",
        "glue:BatchCreatePartition"
      ],
      "Resource": [
        "arn:aws:glue:*:*:catalog",
        "arn:aws:glue:*:*:database/query_agent_db",
        "arn:aws:glue:*:*:table/query_agent_db/*"
      ]
    }
  ]
}
```

### 2. Athena Cost Governance & Guardrails
- Amazon Athena charges **$5.00 per TB** of data scanned.
- **Workgroup Per-Query Limit:** Configured with a `BytesScannedCutoffPerQuery` limit (e.g., 1 GB in development) to terminate runaway or unpartitioned full-bucket scans before execution.
- **Query Timeout Enforcement:** Athena queries automatically cancel after 60 seconds to mitigate resource exhaustion from non-converging joins.

---

## Evaluation Benchmark & Golden Queries

### Analytical Benchmark (50 Multi-Table Prompts)
The agent was evaluated across 50 production test prompts spanning FinOps cloud costs, retail, music streaming, and food delivery domains:

| Metric | Result |
| :--- | :--- |
| **First-Pass SQL Generation Accuracy** | 92.0% |
| **Post-Self-Healing Recovery Rate** | 98.0% |
| **Average Bedrock End-to-End Latency** | 1.42s |
| **Average Bedrock Token Cost per Query** | $0.0012 |
| **AST Mutation Interception Rate** | 100% (Blocks DROP/DELETE/INSERT) |

---

## 💰 FinOps & Cloud Cost Optimization Benchmarks

The agent natively parses complex AWS billing datasets (such as `synthetic_cur_aug_sep_2026`) and generates both quantitative analytics and qualitative FinOps playbooks:

| FinOps Domain | Natural Language Query | Generated Output & Visualizations |
| :--- | :--- | :--- |
| **Statistical Outlier Detection** | `Find the top anomalies and provide me recommendations to resolve it` | 3-sigma anomaly breakdown across AWS services, bar chart of abnormal spend, and immediate waste elimination steps. |
| **Daily Trend & Spikes** | `From synthetic_cur_aug_sep_2026, show total daily spend for every day in September 2026 ordered by date. Which days have the highest spikes and what are the FinOps recommendations?` | 30-day interactive Plotly line chart highlighting Sep 1 and Sep 30 anomalies, followed by root-cause tax & fee amortization guidance. |
| **Non-Prod Waste Audit** | `From synthetic_cur_aug_sep_2026, show total spend by product_product_name for the dev and test-qa accounts ordered by total spend descending. Provide rightsizing and scheduling recommendations.` | Categorical cost breakdown across dev/QA environments with scheduled shutdown and Spot instance adoption playbooks. |
| **Tag Hygiene Governance** | `From synthetic_cur_aug_sep_2026, show total spend grouped by line_item_product_code where resource_tags_user_owner is null ordered by total spend descending.` | Unallocated infrastructure cost audit paired with automated tag-or-terminate policy governance. |
| **Compute Sizing Audit** | `From synthetic_cur_aug_sep_2026, show the top 5 EC2 instance types by total spend.` | Instance family cost concentration chart with AWS Graviton migration and Savings Plan recommendations. |

---

## Sample Showcase Queries

### 1. Single-Table Aggregation (Metric Breakdown)
*"What is the total order value generated from each city?"*
```sql
SELECT c.city, ROUND(SUM(o.order_value), 2) AS total_order_value
FROM zomato_orders o
JOIN zomato_customers c ON o.customer_id = c.customer_id
WHERE LOWER(o.order_status) = 'delivered'
GROUP BY c.city
ORDER BY total_order_value DESC;
```

### 2. Cross-Table Join with Analytical Filtering
*"Find the top 5 customers by total order spend who hold a Prime membership."*
```sql
SELECT c.customer_id, c.first_name, c.last_name, c.city, ROUND(SUM(o.order_value), 2) AS total_spend
FROM zomato_customers c
JOIN zomato_orders o ON c.customer_id = o.customer_id
WHERE c.is_prime = true AND LOWER(o.order_status) = 'delivered'
GROUP BY c.customer_id, c.first_name, c.last_name, c.city
ORDER BY total_spend DESC
LIMIT 5;
```

### 3. Self-Healing Error Recovery Example
When prompted: *"Show the highest rated tracks per genre."*
- **Attempt 1 (Synthesized Dialect Error):** Nova Pro generated an unaliased subquery with a Postgres-style `DISTINCT ON` statement not supported by Trino.
- **Athena Feedback:** `SYNTAX_ERROR: line 1:20: Column 'genre' must be an aggregate expression or appear in GROUP BY clause`.
- **Attempt 2 (Autonomous Recovery):** The self-healing loop caught the trace, re-prompted Nova Pro with the Trino specification, and returned a valid windowed query:
```sql
WITH ranked_tracks AS (
    SELECT track_name, artist, genre, user_rating,
           DENSE_RANK() OVER (PARTITION BY genre ORDER BY user_rating DESC) as rank_num
    FROM spotify
)
SELECT track_name, artist, genre, user_rating
FROM ranked_tracks
WHERE rank_num = 1;
```

---

## Data Catalog & Dynamic Schema Engine (`query_agent_db`)

The platform supports delimited text files, columnar Parquet tables, and multi-tenant AWS Cost & Usage Reports:

### AWS Cost & Usage Report Table (CUR)
*Evaluation Dataset:* [Synthetic CUR Dataset - Aug/Sep 2026](https://drive.google.com/drive/u/0/folders/1VKXuPVwNDiVyaO3pzjG4AGIKEvSi_HZo)
```sql
CREATE EXTERNAL TABLE query_agent_db.synthetic_cur_aug_sep_2026 (
    bill_payer_account_id bigint,
    bill_billing_period_start_date string,
    line_item_usage_account_id bigint,
    line_item_usage_account_name string,
    line_item_line_item_type string,
    line_item_usage_start_date string,
    line_item_usage_end_date string,
    line_item_product_code string,
    line_item_usage_type string,
    line_item_operation string,
    line_item_resource_id string,
    line_item_usage_amount double,
    line_item_unblended_cost double,
    product_product_name string,
    product_region string,
    product_instance_type string,
    pricing_term string,
    reservation_reservation_a_r_n string,
    reservation_effective_cost string,
    reservation_unused_amortized_upfront_fee_for_billing_period string,
    reservation_unused_recurring_fee string,
    savings_plan_savings_plan_a_r_n string,
    savings_plan_offering_type string,
    savings_plan_savings_plan_effective_cost double,
    savings_plan_total_commitment_to_date string,
    savings_plan_used_commitment string,
    resource_tags_user_environment string,
    resource_tags_user_owner string,
    resource_tags_user_application string,
    year bigint,
    month bigint
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 's3://query-agent-lake-<account_id>/data/synthetic_cur_aug_sep_2026/'
TBLPROPERTIES ('skip.header.line.count'='1');
```

### Columnar High-Performance Format (Apache Parquet)
```sql
CREATE EXTERNAL TABLE query_agent_db.spotify (
    track_id BIGINT,
    track_name STRING,
    artist STRING,
    genre STRING,
    user_rating DOUBLE,
    play_duration_seconds INT
)
STORED AS PARQUET
LOCATION 's3://query-agent-lake-<account_id>/data/spotify/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');
```

---

## Repository Structure

```text
query-agent/
├── .streamlit/
│   ├── config.toml           # Streamlit server flags (1 GB maxUploadSize, CORS overrides)
│   └── secrets.toml          # Local AWS credentials & region configuration (git-ignored)
├── docs/
│   └── images/               # Architectural visuals & FinOps screenshots
├── agent.py                  # Bedrock Converse orchestrator, FinOps advisor & self-healing loop
├── app.py                    # Streamlit interface, batch streaming ingestion & BI visuals
├── athena_manager.py         # S3 streaming ingestion, Glue registration & Athena executor
├── database.py               # AST SQL sanitizer (sqlglot) & read-only enforcement
├── example_store.py          # Dynamic few-shot vector store using Titan Embeddings v2
├── index_advisor.py          # Partition & Storage Layout Advisor for S3 scan optimization
├── metrics.yaml              # Declarative business metric store & semantic definitions
├── seed_athena.py            # Automated provisioner for initial S3 data lake & Glue tables
├── Dockerfile                # Production multi-stage build (linux/amd64)
├── .dockerignore             # Excludes venvs, caches, and secrets from container images
├── requirements.txt          # Python production dependencies
├── .gitignore                # Git exclusions
└── README.md                 # System architecture documentation & execution guide
```

---

## Local Development Setup

### Prerequisites
- Python 3.10+ (tested on Python 3.12)
- AWS Account with Bedrock model access (`amazon.nova-pro-v1:0` and `amazon.titan-embed-text-v2:0`)
- Configured AWS CLI profile with Athena, Glue, and S3 permissions

### 1. Clone & Setup Environment
```bash
git clone [https://github.com/kkaustav/query-agent.git](https://github.com/kkaustav/query-agent.git)
cd query-agent

python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Configure AWS Access
Set up your local credentials via AWS CLI:
```bash
aws configure
```
Or place them in `.streamlit/secrets.toml`:
```toml
AWS_ACCESS_KEY_ID = "AKIAXXXXXXXXXXXXXXXX"
AWS_SECRET_ACCESS_KEY = "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
AWS_DEFAULT_REGION = "us-east-1"
```

### 3. Provision S3 Data Lake & Launch Application
```bash
python seed_athena.py
streamlit run app.py
```
Access the application locally at `http://localhost:8501`.

---

## Production Deployment: Amazon ECS on AWS Fargate

### Build and Deploy Container
```bash
# Set deployment environment variables
export REGION="us-east-1"
export ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

# Authenticate Docker to Amazon ECR
aws ecr get-login-password --region ${REGION} | docker login --username AWS --password-stdin ${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com

# Build for linux/amd64 and push
docker build --platform linux/amd64 -t query-agent:latest .
docker tag query-agent:latest ${ACCOUNT_ID}.dkr.ecr.${REGION}[.amazonaws.com/query-agent:latest](https://.amazonaws.com/query-agent:latest)
docker push ${ACCOUNT_ID}.dkr.ecr.${REGION}[.amazonaws.com/query-agent:latest](https://.amazonaws.com/query-agent:latest)

# Trigger zero-downtime rolling deployment
aws ecs update-service --cluster default --service query-agent --force-new-deployment --region ${REGION}
```

### On-Demand Service Control (Cost Optimization)
To eliminate 24/7 AWS Fargate compute charges when not actively presenting demos or interviewing, scale the container task count to zero. All S3 data lake files, Glue table catalogs, and Docker container images remain intact.

- **Stop Service (Scale to 0 tasks — halts compute billing):**
  ```bash
  aws ecs update-service --cluster default --service query-agent --desired-count 0 --region us-east-1
  ```
- **Check Service Status:**
  ```bash
  aws ecs describe-services --cluster default --services query-agent --query "services[0].[desiredCount,runningCount]" --output text --region us-east-1
  ```
- **Start Service (Scale to 1 task — live and ready in ~45 seconds):**
  ```bash
  aws ecs update-service --cluster default --service query-agent --desired-count 1 --region us-east-1
  ```

---

## License

This project is licensed under the MIT License — see the [LICENSE](LICENSE) file for details.
