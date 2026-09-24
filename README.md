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
> *(Note: The AWS ECS Fargate task is scaled to 0 tasks when idle to optimize cloud costs; can be scaled to 1 on-demand via AWS CLI).*

An enterprise-grade, serverless Text-to-SQL conversational analytics platform. Translates natural language inquiries into Trino/Presto SQL, executes queries against an Amazon S3 data lake using Amazon Athena, dynamically registers schemas via AWS Glue Data Catalog, and provisions interactive Plotly visualizations with executive business insights.

Engineered with Docker, published to **Amazon ECR**, and hosted on **Amazon ECS Express Mode** backed by AWS Fargate with managed ALB routing and keyless IAM Task Role authentication for **Amazon Bedrock**.

---

## Architecture Overview

The platform decouples streaming data ingestion, in-context vector retrieval, serverless query execution, and conversational business intelligence:

```mermaid
flowchart TD
    subgraph ClientLayer["Presentation & Interaction Layer (app.py)"]
        UI["Streamlit Interface<br/>Multi-Tab Session State"]
        Visuals["BI Analytics Engine<br/>Plotly Visualizations + CSV/XLSX Export"]
        Uploader["Batch Streaming Ingestion Engine<br/>(10+ Files Simultaneous • Up to 4 TB per file)"]
        History["Browser-Style Chat History<br/>(Isolated Multi-Tab Navigation via Query Params)"]
    end

    subgraph InfrastructureLayer["AWS ECS Runtime Environment"]
        ALB["Application Load Balancer<br/>(HTTPS Managed TLS)"]
        ECSTask["ECS Fargate Task<br/>(Docker: linux/amd64)"]
        IAMRole["IAM Task Role<br/>(Keyless Bedrock, Athena, S3, Glue Auth)"]
    end

    subgraph IntelligenceLayer["Agent & RAG Orchestration (agent.py)"]
        Agent["NLQueryAgent<br/>Converse API Orchestrator"]
        VectorStore["Dynamic Few-Shot Store<br/>(example_store.py)"]
        JoinEngine["Cross-Table Join Engine<br/>(Dynamic Foreign Key Resolution)"]
        SelfHealing["Self-Healing Reflection Loop<br/>(Diagnostic Tracing & Syntax Repair)"]
    end

    subgraph BedrockLayer["Amazon Bedrock Foundation Models"]
        Titan["Amazon Titan Text Embeddings v2<br/>(amazon.titan-embed-text-v2:0)"]
        Nova["Amazon Nova Pro 1.0<br/>(amazon.nova-pro-v1:0)"]
    end

    subgraph DataLakehouseLayer["Serverless Storage & Execution (athena_manager.py)"]
        S3Data[("Amazon S3 Data Lake<br/>s3://query-agent-lake-ACCOUNT_ID/data/")]
        Glue["AWS Glue Data Catalog<br/>(query_agent_db)"]
        Athena["Amazon Athena<br/>(Trino/Presto Query Engine)"]
        S3Results[("Amazon S3 Query Results<br/>s3://query-agent-lake-ACCOUNT_ID/athena-results/")]
    end

    %% Network & Access
    ALB -->|"Route Port 8501"| ECSTask
    ECSTask --- IAMRole
    ECSTask --> UI

    %% Streaming Ingestion Flow
    Uploader -->|"1. Batch Multi-part Chunk Stream"| S3Data
    S3Data -->|"2. Schema Registration DDL"| Glue
    Glue -.->|"Register External Tables"| Athena

    %% User Query Flow
    UI -->|"3. Natural Language Question"| Agent

    %% Vector RAG
    Agent -->|"4. Embedding Query"| Titan
    Titan -->|"Cosine Similarity Match"| VectorStore
    VectorStore -->|"5. Top-K Dialect SQL Templates"| Agent

    %% SQL Synthesis & Reflection
    Agent -->|"6. Inspect Cross-Table Schemas"| JoinEngine
    JoinEngine -->|"Synthesize Trino SQL + Explicit Joins"| Nova
    Nova -->|"7. Generated SQL Statement"| Agent
    Agent -->|"8. Execute Query (DDL/DML Decoupled)"| Athena

    %% Athena Execution
    Athena -->|"Inspect Schema"| Glue
    Athena -->|"Scan Partitions"| S3Data
    Athena -->|"Stage CSV Results"| S3Results
    S3Results -->|"9. Load DataFrames"| Agent

    %% Self-Healing Feedback
    Athena -.->|"Runtime/Syntax Error Trace"| SelfHealing
    SelfHealing -.->|"Re-prompt with Error Diagnostic"| Nova

    %% Output & History
    Agent -->|"10. Results & Visuals"| UI
    Agent -->|"11. Generate Executive Summary"| Nova
    Nova -->|"Executive Insight"| Visuals
    UI --> History
```

---

## Key System Features

### 1. Serverless Lakehouse Architecture (Amazon S3 + Amazon Athena)
* Replaces container-bound databases with a scalable data lake (`s3://query-agent-lake-<account_id>/`).
* Executes distributed SQL across tabular datasets using Amazon Athena's serverless Trino/Presto engine.
* Removes container RAM and disk limits; stores query results directly in S3 result staging prefixes.
* Handles DDL (`CREATE EXTERNAL TABLE`, `DROP TABLE`) and DML (`SELECT`) operations separately to prevent S3 staging collisions.

### 2. High-Capacity Batch Streaming Ingestion (10+ Mixed Datasets, Up to 4 TB)
* Direct multi-part streaming ingestion via Boto3 to eliminate Out-Of-Memory (OOM) risks on small container runtimes.
* Ingests **CSV**, **TSV**, **XLSX**, **XLS**, and columnar **PARQUET** datasets concurrently with live progress tracking.
* In-memory stream isolation prevents closed-file I/O exceptions while automatically mapping Pandas dtypes to Athena column definitions.
* Drops and recreates table definitions dynamically in the **AWS Glue Data Catalog** (`query_agent_db`) without metadata collisions.

### 3. Cross-Table Relational Join Intelligence
* Bedrock Nova Pro inspects all registered catalog schemas simultaneously to identify foreign key relationships.
* Resolves columns residing across separate datasets (e.g., `city` in customer profiles joined with `order_value` in transaction logs) using explicit Presto/Trino SQL `JOIN` clauses.
* Eliminates `COLUMN_NOT_FOUND` runtime exceptions through schema-aware reasoning and self-healing error traces.

### 4. Isolated Multi-Tab Chat History
* Browser-style conversation management powered by URL query parameters (`?session=<session_id>`) and a shared application resource cache.
* Opening historical queries renders the selected conversation in a new, independent browser tab without resetting the active query workflow.

### 5. Amazon Nova Pro Reasoning & Dynamic Few-Shot RAG
* Generates Trino-compliant SQL using Amazon Bedrock's flagship `amazon.nova-pro-v1:0` model via the **Converse API**.
* Employs **Amazon Titan Text Embeddings v2** (`amazon.titan-embed-text-v2:0`) to vector-search golden query patterns in-memory, mitigating token bloat while grounding syntax for complex joins and window functions.

### 6. Autonomous Self-Healing Execution Loop
* Intercepts execution failures, schema mismatches, and syntax errors in real time.
* Feeds raw database exceptions and execution traces back into Nova Pro for up to 3 automatic correction attempts before returning output.

### 7. Interactive BI Dashboard & Secure Governance
* Renders instant categorical/numerical breakdowns using dynamic **Plotly Express** charts.
* Exports clean result sets directly to **CSV** and **Microsoft Excel (.xlsx)**.
* Deployed with keyless IAM Task Role authorization; no long-lived AWS keys stored in code or containers.

---

## Repository Structure

```text
query-agent/
├── .streamlit/
│   ├── config.toml           # Streamlit server flags (4 TB maxUploadSize, CORS overrides)
│   └── secrets.toml          # Local AWS credentials & region configuration (git-ignored)
├── agent.py                  # Bedrock Converse orchestrator, join engine & self-healing loop
├── app.py                    # Streamlit interface, batch multi-file ingestion & BI visuals
├── athena_manager.py         # S3 streaming ingestion, Glue registration & Athena executor
├── database.py               # AST SQL sanitizer & planner cost validation utilities
├── example_store.py          # Dynamic few-shot vector store using Titan Embeddings v2
├── index_advisor.py          # Query pattern tracker & automated tuning advisor
├── seed_athena.py            # Automated provisioner for initial S3 data lake & Glue tables
├── Dockerfile                # Production multi-stage build (linux/amd64)
├── .dockerignore             # Excludes venvs, caches, and secrets from container images
├── requirements.txt          # Python production dependencies
├── .gitignore                # Git exclusions
└── README.md                 # System architecture documentation & execution guide
```

---

## Data Catalog & Dynamic Schema Engine (`query_agent_db`)

The platform supports both seeded baseline tables and on-the-fly multi-file catalog registrations:

### Baseline Seeded E-Commerce Tables
```sql
CREATE EXTERNAL TABLE query_agent_db.customers (
    customer_id INT,
    name STRING,
    email STRING,
    country STRING,
    created_at STRING
)
STORED AS TEXTFILE
LOCATION 's3://query-agent-lake-<account_id>/data/customers/';

CREATE EXTERNAL TABLE query_agent_db.products (
    product_id INT,
    product_name STRING,
    category STRING,
    price DOUBLE,
    stock INT
)
STORED AS TEXTFILE
LOCATION 's3://query-agent-lake-<account_id>/data/products/';

CREATE EXTERNAL TABLE query_agent_db.orders (
    order_id INT,
    customer_id INT,
    order_date STRING,
    status STRING,
    total_amount DOUBLE
)
STORED AS TEXTFILE
LOCATION 's3://query-agent-lake-<account_id>/data/orders/';

CREATE EXTERNAL TABLE query_agent_db.order_items (
    item_id INT,
    order_id INT,
    product_id INT,
    quantity INT,
    unit_price DOUBLE
)
STORED AS TEXTFILE
LOCATION 's3://query-agent-lake-<account_id>/data/order_items/';
```

### Dynamic Multi-Domain Ingestion
Uploading custom datasets via the sidebar automatically builds Hive-compliant external tables. Supported domains include:
* **Food Delivery & QSR (`zomato_customers`, `zomato_orders`)**: Customer demographic profiles, order statuses, coupons, ratings, and delivery times.
* **Music Streaming (`spotify`)**: User engagement, genre analytics, device types, skip counts, and playback metrics.
* **Marketplace Operations (`vendor`, `vendor_inventory`, `product`, `market_date_info`)**: Multi-table inventory volumes, product sizing, booth allocations, and weather correlation.

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
Set up your local credentials:
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
Access the application at `http://localhost:8501`.

---

## Production Deployment: AWS ECS Express Mode

### 1. Configure IAM Task Role Policies
Attach the required AWS-managed policies to your ECS Task Role (`QueryAgentAppRunnerInstanceRole` or dedicated ECS task role):
* `AmazonBedrockFullAccess`
* `AmazonAthenaFullAccess`
* `AmazonS3FullAccess`
* `AWSGlueConsoleFullAccess`

### 2. Build and Deploy Container
```bash
# Set environment variables
export REGION="us-east-1"
export ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

# Authenticate Docker to Amazon ECR
aws ecr get-login-password --region $REGION | docker login --username AWS --password-stdin $ACCOUNT_ID.dkr.ecr.$REGION.amazonaws.com

# Build for linux/amd64 and push
docker build --platform linux/amd64 -t query-agent:latest .
docker tag query-agent:latest $ACCOUNT_ID.dkr.ecr.$[REGION.amazonaws.com/query-agent:latest](https://REGION.amazonaws.com/query-agent:latest)
docker push $ACCOUNT_ID.dkr.ecr.$[REGION.amazonaws.com/query-agent:latest](https://REGION.amazonaws.com/query-agent:latest)

# Trigger zero-downtime rolling deployment
aws ecs update-service --cluster default --service query-agent --force-new-deployment --region $REGION
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