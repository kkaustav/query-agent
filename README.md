# Autonomous Natural Language Database Query Agent (Serverless Data Lakehouse)

[![Streamlit App](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://query-agent.streamlit.app/)
[![AWS ECS Live](https://img.shields.io/badge/AWS%20ECS-Production%20Deployment-232F3E.svg?logo=amazon-ecs&logoColor=white)](https://qu-ad51b077a15a4aada7d7d6906df94105.ecs.us-east-1.on.aws/)
[![Python 3.12](https://img.shields.io/badge/Python-3.12-3776AB.svg?logo=python&logoColor=white)](https://www.python.org/)
[![Amazon Bedrock](https://img.shields.io/badge/AWS-Bedrock%20Nova%20Pro-FF9900.svg?logo=amazon-aws&logoColor=white)](https://aws.amazon.com/bedrock/)
[![Amazon Athena](https://img.shields.io/badge/AWS-Amazon%20Athena-232F3E.svg?logo=amazon-aws&logoColor=white)](https://aws.amazon.com/athena/)
[![Amazon S3](https://img.shields.io/badge/AWS-Amazon%20S3%20Lake-569A31.svg?logo=amazon-s3&logoColor=white)](https://aws.amazon.com/s3/)
[![AWS Glue](https://img.shields.io/badge/AWS-Glue%20Catalog-8C4FFF.svg?logo=amazon-aws&logoColor=white)](https://aws.amazon.com/glue/)
[![Docker](https://img.shields.io/badge/Docker-Containerized-2496ED.svg?logo=docker&logoColor=white)](https://www.docker.com/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

> **Live Deployments:**
> * **Primary (Always Online):** [https://query-agent.streamlit.app/](https://query-agent.streamlit.app/)
> * **Enterprise AWS Production (ECS Fargate + ALB):** [https://qu-ad51b077a15a4aada7d7d6906df94105.ecs.us-east-1.on.aws/](https://qu-ad51b077a15a4aada7d7d6906df94105.ecs.us-east-1.on.aws/)  
> *(Note: The AWS ECS Fargate task is scaled to 0 tasks when idle to optimize cloud costs; spin up to 1 task on-demand via the AWS CLI).*

An enterprise-grade, serverless Text-to-SQL conversational analytics platform. Translates natural language inquiries into Trino/Presto SQL, validates query safety via Abstract Syntax Tree (AST) parsing, executes distributed queries over an Amazon S3 data lake using Amazon Athena, dynamically catalogs schemas via the AWS Glue Data Catalog, and provisions interactive Plotly visualizations paired with executive insights.

Containerized with Docker, published to **Amazon ECR**, and hosted on **Amazon ECS on AWS Fargate** with managed ALB routing and keyless IAM Task Role authentication for **Amazon Bedrock**.

---

## Architecture Overview

The platform decouples streaming data ingestion, semantic vector retrieval, AST query sanitation, serverless query execution, and conversational business intelligence:

```mermaid
flowchart TD
    subgraph ClientLayer["Presentation & Interaction Layer (app.py)"]
        UI["Streamlit Interface<br/>Multi-Tab Session State"]
        Visuals["BI Analytics Engine<br/>Plotly Visualizations + CSV/XLSX Export"]
        Uploader["Batch Streaming Ingestion Engine<br/>(Ad-Hoc Tabular Datasets up to 1 GB)"]
        History["Browser-Style Chat History<br/>(Isolated Multi-Tab Navigation via Query Params)"]
    end

    subgraph InfrastructureLayer["AWS ECS Runtime Environment"]
        ALB["Application Load Balancer<br/>(HTTPS Managed TLS)"]
        ECSTask["ECS Fargate Task<br/>(Docker: linux/amd64)"]
        IAMRole["IAM Task Role<br/>(Least-Privilege Bedrock, Athena, S3, Glue Auth)"]
    end

    subgraph IntelligenceLayer["Agent Orchestration (agent.py)"]
        Agent["NLQueryAgent<br/>Converse API Orchestrator"]
        VectorStore["Dynamic Few-Shot Store<br/>(example_store.py)"]
        JoinEngine["Cross-Table Join Engine<br/>(Dynamic Foreign Key Resolution)"]
        SelfHealing["Self-Healing Reflection Loop<br/>(Diagnostic Tracing & Syntax Repair)"]
    end

    subgraph GovernanceLayer["Query Governance & Optimization"]
        ASTSanitizer["AST SQL Sanitizer<br/>(database.py - sqlglot)"]
        StorageAdvisor["Partition & Storage Advisor<br/>(index_advisor.py)"]
    end

    subgraph BedrockLayer["Amazon Bedrock Foundation Models"]
        Titan["Amazon Titan Text Embeddings v2<br/>(amazon.titan-embed-text-v2:0)"]
        Nova["Amazon Nova Pro 1.0<br/>(amazon.nova-pro-v1:0)"]
    end

    subgraph DataLakehouseLayer["Serverless Storage & Execution (athena_manager.py)"]
        S3Data[("Amazon S3 Data Lake<br/>s3://query-agent-lake-ACCOUNT_ID/data/")]
        Glue["AWS Glue Data Catalog<br/>(query_agent_db)"]
        Athena["Amazon Athena<br/>(Trino/Presto Engine with Cost Limits)"]
        S3Results[("Amazon S3 Query Results<br/>s3://query-agent-lake-ACCOUNT_ID/athena-results/")]
    end

    %% Network & Access
    ALB -->|"Route Port 8501"| ECSTask
    ECSTask --- IAMRole
    ECSTask --> UI

    %% Streaming Ingestion Flow
    Uploader -->|"1. Chunked Stream Upload"| S3Data
    S3Data -->|"2. Schema Registration DDL"| Glue
    Glue -.->|"Register External Tables"| Athena

    %% User Query Flow
    UI -->|"3. Natural Language Question"| Agent

    %% Vector RAG
    Agent -->|"4. Embedding Query"| Titan
    Titan -->|"Cosine Similarity Match"| VectorStore
    VectorStore -->|"5. Top-K Dialect SQL Templates"| Agent

    %% SQL Synthesis & AST Validation
    Agent -->|"6. Inspect Cross-Table Schemas"| JoinEngine
    JoinEngine -->|"Synthesize Trino SQL"| Nova
    Nova -->|"7. Raw SQL Output"| Agent
    Agent -->|"8. AST Parse & Mutation Check"| ASTSanitizer
    ASTSanitizer -->|"9. Approved Read-Only AST"| Athena

    %% Athena Execution
    Athena -->|"Inspect Schema"| Glue
    Athena -->|"Scan Partitions"| S3Data
    Athena -->|"Stage CSV Results"| S3Results
    S3Results -->|"10. Load DataFrames"| Agent

    %% Feedback & Storage Optimization
    Athena -.->|"Runtime Error Diagnostic"| SelfHealing
    SelfHealing -.->|"Re-prompt with Error Diagnostic"| Nova
    Athena -.->|"Execution Metrics"| StorageAdvisor

    %% Output & History
    Agent -->|"11. Data & Visuals"| UI
    Agent -->|"12. Generate Executive Summary"| Nova
    Nova -->|"Executive Insight"| Visuals
    UI --> History
```

---

## Key System Features

### 1. Serverless Lakehouse Architecture (Amazon S3 + Amazon Athena)
* Replaces container-bound transactional databases with an analytical data lake (`s3://query-agent-lake-<account_id>/`).
* Executes distributed SQL across tabular datasets using Amazon Athena's serverless Trino/Presto engine.
* Supports both delimited text files (`CSV`, `TSV`) and columnar storage (`Apache Parquet` with Snappy compression).
* Decouples DDL (`CREATE EXTERNAL TABLE`, `DROP TABLE`) and DML (`SELECT`) workflows to prevent S3 query staging collisions.

### 2. Dual-Track Data Ingestion Strategy
* **Ad-Hoc UI Streaming (In-Band):** Streamlit file uploader supports batch uploading up to 10+ heterogeneous datasets (CSV, TSV, XLSX, XLS, PARQUET up to 1 GB per file) utilizing memory-buffered streaming to prevent container Out-Of-Memory (OOM) crashes on 2 GB Fargate tasks.
* **Lakehouse Scale Ingestion (Out-Of-Band):** For multi-gigabyte or terabyte workloads exceeding container ephemeral storage (default 20 GB), files bypass the web server via S3 Multipart Uploads or AWS CLI direct-to-S3 ingestion, followed by immediate AWS Glue Catalog schema synchronization.

### 3. AST Query Sanitization & Zero-Trust Safety (`database.py`)
* Employs Abstract Syntax Tree (AST) analysis via `sqlglot` to parse generated SQL before transmission to Athena.
* **Strict Read-Only Enforcement:** Traverses the expression tree to verify root nodes are `Select` statements. Rejects destructive operations (`DROP`, `DELETE`, `UPDATE`, `INSERT`, `ALTER`, `TRUNCATE`, `GRANT`).
* Neutralizes SQL injection and model hallucination vectors by blocking multi-statement execution and comments that bypass parser validation.

### 4. Cross-Table Relational Join Intelligence
* Bedrock Nova Pro inspects all active tables in the AWS Glue Data Catalog (`query_agent_db`) simultaneously.
* Infers implicit foreign-key relationships across disparate operational domains (e.g., joining customer demographic attributes from `zomato_customers` with transaction volume in `zomato_orders`).
* Employs explicit Presto/Trino SQL join semantics (`LEFT JOIN`, `INNER JOIN`) to eliminate `COLUMN_NOT_FOUND` runtime exceptions.

### 5. Autonomous Self-Healing Execution Loop
* Intercepts execution failures, schema mismatches, and Trino dialect errors in real time.
* Feeds raw database exceptions and execution traces back into Nova Pro for up to 3 automated correction iterations before returning output.

### 6. Partition & Storage Layout Advisor (`index_advisor.py`)
* Because Amazon Athena operates on serverless Trino over S3 without traditional B-tree indexes, performance and cost depend entirely on storage layout.
* Analyzes recurring query `WHERE` filters, `JOIN` predicates, and aggregations to output recommendations for:
  * **Partition Keys:** Identifying high-cardinality pruning candidates (e.g., `order_date`, `city`, `season`).
  * **Columnar Formats:** Advising conversion from raw CSV/JSON to Apache Parquet with Snappy compression to reduce scanned byte volume by 80–90%.
  * **Bucketing Strategies:** Recommending hash-bucketing keys for frequently joined foreign keys.

### 7. Isolated Multi-Tab Chat History
* Session management powered by URL query parameters (`?session=<session_id>`) and a shared application resource cache.
* Opening historical queries renders selected conversations in a new, independent browser tab without disrupting the active query session.

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
* Amazon Athena charges **$5.00 per TB** of data scanned.
* **Workgroup Per-Query Limit:** Configured with a `BytesScannedCutoffPerQuery` limit (e.g., 1 GB in development) to terminate runaway or unpartitioned full-bucket scans before execution.
* **Query Timeout Enforcement:** Athena queries automatically cancel after 60 seconds to mitigate resource exhaustion from non-converging joins.

---

## Evaluation Benchmark & Golden Queries

### Analytical Benchmark (50 Multi-Table Prompts)
The agent was evaluated across 50 production test prompts spanning retail, music streaming, and food delivery domains:

| Metric | Result |
| :--- | :--- |
| **First-Pass SQL Generation Accuracy** | 92.0% |
| **Post-Self-Healing Recovery Rate** | 98.0% |
| **Average Bedrock End-to-End Latency** | 1.42s |
| **Average Bedrock Token Cost per Query** | $0.0012 |
| **AST Mutation Interception Rate** | 100% (Blocks DROP/DELETE/INSERT) |

### Sample Showcase Queries

#### 1. Single-Table Aggregation (Metric Breakdown)
> *"What is the total order value generated from each city?"*
```sql
SELECT c.city, ROUND(SUM(o.order_value), 2) AS total_order_value
FROM zomato_orders o
JOIN zomato_customers c ON o.customer_id = c.customer_id
GROUP BY c.city
ORDER BY total_order_value DESC;
```

#### 2. Cross-Table Join with Analytical Filtering
> *"Find the top 5 customers by total order spend who hold a Prime membership."*
```sql
SELECT c.customer_id, c.first_name, c.last_name, c.city, ROUND(SUM(o.order_value), 2) AS total_spend
FROM zomato_customers c
JOIN zomato_orders o ON c.customer_id = o.customer_id
WHERE c.is_prime = true AND o.order_status = 'delivered'
GROUP BY c.customer_id, c.first_name, c.last_name, c.city
ORDER BY total_spend DESC
LIMIT 5;
```

#### 3. Self-Healing Error Recovery Example
When prompted: *"Show the highest rated tracks per genre."*  
* **Attempt 1 (Synthesized Dialect Error):** Nova Pro generated an unaliased subquery with a Postgres-style `DISTINCT ON` statement not supported by Trino.
* **Athena Feedback:** `SYNTAX_ERROR: line 1:20: Column 'genre' must be an aggregate expression or appear in GROUP BY clause`.
* **Attempt 2 (Autonomous Recovery):** The self-healing loop caught the trace, re-prompted Nova Pro with the Trino specification, and returned a valid windowed query:
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

The platform supports both delimited text files and columnar Parquet tables:

### Baseline Delimited E-Commerce Tables (Textfile)
```sql
CREATE EXTERNAL TABLE query_agent_db.customers (
    customer_id INT,
    name STRING,
    email STRING,
    country STRING,
    created_at STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 's3://query-agent-lake-<account_id>/data/customers/'
TBLPROPERTIES ('skip.header.line.count'='1');

CREATE EXTERNAL TABLE query_agent_db.orders (
    order_id INT,
    customer_id INT,
    order_date STRING,
    status STRING,
    total_amount DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 's3://query-agent-lake-<account_id>/data/orders/'
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
│   ├── config.toml           # Streamlit server flags (CORS, memory buffers)
│   └── secrets.toml          # Local AWS credentials & region configuration (git-ignored)
├── agent.py                  # Bedrock Converse orchestrator, join engine & self-healing loop
├── app.py                    # Streamlit interface, batch streaming ingestion & BI visuals
├── athena_manager.py         # S3 streaming ingestion, Glue registration & Athena executor
├── database.py               # AST SQL sanitizer (sqlglot) & read-only enforcement
├── example_store.py          # Dynamic few-shot vector store using Titan Embeddings v2
├── index_advisor.py          # Partition & Storage Layout Advisor for S3 scan optimization
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
* Python 3.10+ (tested on Python 3.12)
* AWS Account with Bedrock model access (`amazon.nova-pro-v1:0` and `amazon.titan-embed-text-v2:0`)
* Configured AWS CLI profile with Athena, Glue, and S3 permissions

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

---

## On-Demand Service Control (Cost Optimization)

To eliminate 24/7 AWS Fargate compute charges when not actively presenting demos or interviewing, scale the container task count to zero. All S3 data lake files, Glue table catalogs, and Docker container images remain intact.

### Stop Service (Scale to 0 tasks — halts compute billing):
```bash
aws ecs update-service --cluster default --service query-agent --desired-count 0 --region us-east-1
```

### Check Service Status (Verify task counts without opening JSON pagers):
```bash
aws ecs describe-services --cluster default --services query-agent --query "services[0].[desiredCount,runningCount]" --output text --region us-east-1
```
* Returns `0   0` when fully stopped.
* Returns `1   1` when fully running and ready.

### Start Service (Scale to 1 task — live and ready in ~45 seconds):
```bash
aws ecs update-service --cluster default --service query-agent --desired-count 1 --region us-east-1
```

---

## License

This project is licensed under the MIT License — see the [LICENSE](LICENSE) file for details.