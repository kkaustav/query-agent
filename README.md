# Autonomous Natural Language Database Query Agent (Text-to-SQL)

[![Python 3.12](https://img.shields.io/badge/Python-3.12-3776AB.svg?logo=python&logoColor=white)](https://www.python.org/)
[![AWS Bedrock](https://img.shields.io/badge/AWS-Bedrock%20Nova%20Pro-FF9900.svg?logo=amazon-aws&logoColor=white)](https://aws.amazon.com/bedrock/)
[![Amazon ECS](https://img.shields.io/badge/AWS-ECS%20Express%20Mode-FF9900.svg?logo=amazon-aws&logoColor=white)](https://aws.amazon.com/ecs/)
[![Docker](https://img.shields.io/badge/Docker-Containerized-2496ED.svg?logo=docker&logoColor=white)](https://www.docker.com/)
[![SQLAlchemy](https://img.shields.io/badge/SQLAlchemy-2.0-D71F00.svg?logo=sqlalchemy&logoColor=white)](https://www.sqlalchemy.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

An enterprise-grade autonomous Text-to-SQL conversational agent that translates complex business questions into dialect-optimized SQL, validates query execution plans via pre-execution cost guardrails, self-heals runtime errors dynamically, supports multi-format live data ingestion, and renders interactive analytical visualizations with executive summaries.

Containerized with Docker, published to **Amazon ECR**, and hosted via **Amazon ECS Express Mode** with managed ALB routing and keyless IAM Task Role authentication for **Amazon Bedrock**.

---

## Architecture Overview

The system decouples data ingestion, in-context vector retrieval, query cost governance, autonomous self-healing execution, and business intelligence reporting:

```mermaid
flowchart TD
    subgraph Client["Client & Presentation Layer (app.py)"]
        UI["Streamlit Interface<br/>Multi-Turn Chat"]
        Visuals["Analytics & BI Engine<br/>Plotly Visualizations + CSV/Excel Exports"]
        Uploader["Multi-Format Ingestion Engine<br/>(CSV, Excel, Parquet, JSON, SQLite)"]
    end

    subgraph Infrastructure["Container & Cloud Runtime (AWS ECS Express Mode)"]
        ALB["Application Load Balancer<br/>(HTTPS Managed TLS)"]
        ECSTask["ECS Fargate Task<br/>(Docker Container: linux/amd64)"]
        IAMRole["IAM Task Role<br/>(Keyless Bedrock Authorization)"]
    end

    subgraph Intelligence["Agent & RAG Orchestration Layer (agent.py)"]
        Agent["NLQueryAgent<br/>Converse API Orchestrator"]
        VectorStore["Dynamic Few-Shot Store<br/>(example_store.py)"]
        Advisor["Index Advisor Registry<br/>(index_advisor.py)"]
    end

    subgraph FoundationModels["AWS Bedrock Runtime Engine"]
        Titan["Amazon Titan Text Embeddings v2<br/>(amazon.titan-embed-text-v2:0)"]
        Nova["Amazon Nova Pro 1.0<br/>(amazon.nova-pro-v1:0)"]
    end

    subgraph DataGovernance["Database Engine & Governance (database.py)"]
        Guardrails["Query Sanitizer<br/>Zero-Trust AST & DDL/DML Block"]
        PlannerCost["Cost & Plan Inspector<br/>EXPLAIN Cost Analysis"]
        DB[(Target Relational DB<br/>SQLite Sandbox / RDS / Aurora)]
    end

    %% Ingestion Flow
    Uploader -->|"Parse & Write Tables"| DB

    %% Network Routing
    ALB -->|"Route Port 8501"| ECSTask
    ECSTask --- IAMRole
    ECSTask --> UI

    %% Query Pipeline
    UI -->|"1. Natural Language Prompt"| Agent

    %% Dynamic Few-Shot RAG
    Agent -->|"2. Vector Search (Query Embedding)"| VectorStore
    VectorStore -->|"Vectorize & Cosine Similarity"| Titan
    Titan -->|"Top-K Reference Vectors"| VectorStore
    VectorStore -->|"3. Top-K Syntax Examples"| Agent

    %% SQL Synthesis
    Agent -->|"4. Converse API (Prompt + Context + Schema)"| Nova
    Nova -->|"5. Synthesized Dialect SQL"| Agent

    %% Governance & Guardrails
    Agent -->|"6. Inspect Query"| Guardrails
    Guardrails -->|"Read-Only Verified"| PlannerCost
    PlannerCost -->|"7. EXPLAIN Plan Evaluation"| DB

    %% Self-Healing Feedback Loop
    PlannerCost -->|"Cost Ceiling Exceeded / Cartesian Scan"| Agent
    DB -->|"Execution Syntax/Runtime Error"| Agent
    Agent -.->|"Correction Feedback Loop (Max 3 Retries)"| Nova

    %% Execution & Advisory
    DB -->|"8. Result Set (DataFrame)"| UI
    DB -->|"9. Query Plan Metrics"| Advisor
    Advisor -.->|"Unindexed Scan Threshold Exceeded"| Nova
    Nova -.->|"Draft Index Creation DDL"| Advisor
    Advisor -->|"Tuning Alert & DDL"| UI

    %% BI & Reporting
    UI -->|"Generate Summary"| Nova
    Nova -->|"Executive Insight"| UI
    UI --> Visuals
```

---

## Key System Features

### 1. Amazon Nova Pro Reasoning Engine
* Powered by Bedrock's flagship model `amazon.nova-pro-v1:0` using the structured **Converse API**.
* Context-aware multi-turn reasoning that retains session history to support iterative query refinements (e.g., *"Filter that to Europe only"* or *"Break that down by month"*).
* Generates dialect-optimized SQL tailored to the active database engine (**SQLite**, **PostgreSQL**, **MySQL**).

### 2. Multi-Format Live Data Ingestion
* Drag-and-drop ingestion directly from the UI sidebar supporting:
  * **Tabular & Delimited:** `.csv`, `.tsv`
  * **Spreadsheets:** `.xlsx`, `.xls` (multi-sheet workbooks automatically ingested as individual relational tables)
  * **Columnar Formats:** `.parquet`
  * **Semi-Structured:** `.json`, `.jsonl`
  * **Relational Sandboxes:** Live hot-swapping of pre-built `.db`, `.sqlite`, and `.sqlite3` databases.
* Automatically sanitizes SQL table names, registers column types, and dynamically refreshes the agent's schema context.

### 3. Dynamic Few-Shot In-Context Retrieval (RAG)
* Avoids static prompt token bloat by dynamically vectorizing verified schema query examples using **Amazon Titan Text Embeddings v2** (`amazon.titan-embed-text-v2:0`).
* Calculates in-memory cosine similarity against golden SQL templates to ground LLM outputs in accurate syntax patterns for complex joins, aggregations, and window functions.

### 4. Pre-Execution Cost Guardrails & AST Sanitization
* **Zero-Trust AST Sanitization:** Rejects mutation or destructive operations (`DROP`, `DELETE`, `UPDATE`, `INSERT`, `ALTER`, `TRUNCATE`, `GRANT`).
* **Pre-Execution `EXPLAIN` Cost Inspector:**
  * **PostgreSQL:** Evaluates `EXPLAIN (FORMAT JSON)` total planner cost against a safety threshold (ceiling: `25,000.0`).
  * **MySQL:** Parses JSON execution plans to inspect optimizer `query_cost` (ceiling: `1,000.0`).
  * **SQLite:** Analyzes `EXPLAIN QUERY PLAN` opcodes to block Cartesian products and unindexed full-table scans before query execution.

### 5. Autonomous Self-Healing Reflection Loop
* When a query encounters execution timeouts, missing column references, or planner rejections, the agent initiates an automated reflection cycle (up to 3 attempts).
* Raw database exceptions and execution traces are injected into a structured self-correction prompt, guiding Nova Pro to repair the query autonomously.

### 6. Automated Database Tuning & Index Advisor
* Background registry tracks repeated full-table scans (`SCAN TABLE`) across user interactions.
* When scan counts exceed configurable thresholds, Nova Pro analyzes query access patterns against table schemas to synthesize optimal index creation DDL (e.g., `CREATE INDEX idx_orders_customer ON orders(customer_id);`).

### 7. Interactive BI Dashboard
* **Dynamic Visualizations:** Plotly charts automatically detect categorical dimensions and numeric metrics to render relevant bar charts.
* **Executive Summaries:** Nova Pro generates concise natural language takeaways beneath query tables.
* **Data Export:** Export query results to CSV or formatted Microsoft Excel (`.xlsx`) workbooks.

---

## Repository Structure

```text
query-agent/
├── .streamlit/
│   └── secrets.toml          # Local AWS credentials & region configuration (git-ignored)
├── data/
│   └── ecommerce.db          # Pre-seeded SQLite relational database
├── agent.py                  # Core Bedrock Converse orchestrator & self-healing loop
├── app.py                    # Clean Streamlit UI, multi-format ingestion & BI visualizations
├── database.py               # SQLAlchemy engine, dialect manager & AST cost guardrails
├── example_store.py          # Dynamic few-shot RAG via Titan Text Embeddings v2
├── index_advisor.py          # Full-table scan tracker and automated DDL advisor
├── seed_data.py              # Automated schema generator and relational data seeder
├── Dockerfile                # Multi-stage production container build (linux/amd64)
├── .dockerignore             # Protects credentials, venvs, and local cache from builds
├── requirements.txt          # Python production dependencies
├── .gitignore                # Git exclusions
└── README.md                 # System architecture documentation & execution guide
```

---

## Relational Schema (Sandbox)

The default sandbox runs an e-commerce schema modeled in SQLite (`data/ecommerce.db`):

```sql
CREATE TABLE customers (
  customer_id INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  email TEXT NOT NULL,
  country TEXT NOT NULL,
  created_at DATETIME
);

CREATE TABLE products (
  product_id INTEGER PRIMARY KEY,
  product_name TEXT NOT NULL,
  category TEXT NOT NULL,
  price REAL NOT NULL,
  stock INTEGER NOT NULL
);

CREATE TABLE orders (
  order_id INTEGER PRIMARY KEY,
  customer_id INTEGER NOT NULL,
  order_date DATETIME,
  status TEXT NOT NULL,
  total_amount REAL NOT NULL,
  FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

CREATE TABLE order_items (
  item_id INTEGER PRIMARY KEY,
  order_id INTEGER NOT NULL,
  product_id INTEGER NOT NULL,
  quantity INTEGER NOT NULL,
  unit_price REAL NOT NULL,
  FOREIGN KEY (order_id) REFERENCES orders(order_id),
  FOREIGN KEY (product_id) REFERENCES products(product_id)
);
```

---

## Local Development Setup

### Prerequisites
* Python 3.10+ (tested on Python 3.12)
* AWS Account with Amazon Bedrock access enabled for:
  * `amazon.nova-pro-v1:0`
  * `amazon.titan-embed-text-v2:0`

### 1. Clone & Setup Environment
```bash
git clone [https://github.com/kkaustav/query-agent.git](https://github.com/kkaustav/query-agent.git)
cd query-agent

python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Configure Credentials
Configure your local environment using the AWS CLI:
```bash
aws configure
```
Or define them in `.streamlit/secrets.toml`:
```toml
AWS_ACCESS_KEY_ID = "AKIAXXXXXXXXXXXXXXXX"
AWS_SECRET_ACCESS_KEY = "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
AWS_DEFAULT_REGION = "us-east-1"
```

### 3. Seed Database & Launch
```bash
python seed_data.py
streamlit run app.py
```
Access the application at `http://localhost:8501`.

---

## Production Deployment: AWS ECS Express Mode

The application is containerized and runs on **Amazon ECS Express Mode** backed by AWS Fargate, fronted by an Application Load Balancer with automated HTTPS certificates.

### 1. Build & Push Image to Amazon ECR
```bash
# Set environment variables
export REGION="us-east-1"
export ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

# Authenticate Docker with Amazon ECR
aws ecr get-login-password --region $REGION | docker login --username AWS --password-stdin $ACCOUNT_ID.dkr.ecr.$REGION.amazonaws.com

# Create repository (one-time)
aws ecr create-repository --repository-name query-agent --region $REGION

# Build for linux/amd64 and push
docker build --platform linux/amd64 -t query-agent:latest .
docker tag query-agent:latest $ACCOUNT_ID.dkr.ecr.$[REGION.amazonaws.com/query-agent:latest](https://REGION.amazonaws.com/query-agent:latest)
docker push $ACCOUNT_ID.dkr.ecr.$[REGION.amazonaws.com/query-agent:latest](https://REGION.amazonaws.com/query-agent:latest)
```

### 2. Configure IAM Task Role
Attach `AmazonBedrockFullAccess` to the ECS Task Role (`QueryAgentAppRunnerInstanceRole` or dedicated ECS task role) with the following trust policy:
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "ecs-tasks.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
```

### 3. Launch via Amazon ECS Express Mode
* **Container Image URI:** `<ACCOUNT_ID>.dkr.ecr.us-east-1.amazonaws.com/query-agent:latest`
* **Container Port:** `8501`
* **Health Check Path:** `/_stcore/health`
* **Compute:** `1 vCPU`, `2 GB Memory`
* **Task Role:** IAM Role with Bedrock permissions (keyless runtime authorization)
* **Auto Scaling:** Minimum `1` task, Maximum `2` tasks

---

## Security & Execution Governance

* **Keyless AWS Authorization:** When running in Amazon ECS, boto3 assumes the IAM Task Role via the container metadata service. No long-lived access keys are baked into images or stored on disk.
* **Zero-Trust AST Sanitizer:** Rejects non-query statements (`INSERT`, `UPDATE`, `DELETE`, `DROP`, `ALTER`, `TRUNCATE`) before any database driver executes.
* **Dialect-Specific Query Execution Timeouts:**
  * **SQLite:** Enforced via Python C-extension opcode progress handlers (`set_progress_handler`).
  * **PostgreSQL:** Configured with `SET statement_timeout = 5000;`.
  * **MySQL:** Enforced with `SET SESSION max_execution_time = 5000;`.
* **Read-Only Transaction Isolation:** Managed connections enforce read-only execution modes to prevent runtime data alteration.

---

## License

This project is licensed under the MIT License — see the [LICENSE](LICENSE) file for details.