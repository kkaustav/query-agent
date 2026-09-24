# Autonomous Natural Language Database Query Agent (Text-to-SQL)

[![Streamlit App](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://query-agent.streamlit.app)
[![Python 3.12](https://img.shields.io/badge/Python-3.12-3776AB.svg?logo=python&logoColor=white)](https://www.python.org/)
[![AWS Bedrock](https://img.shields.io/badge/AWS-Bedrock%20Nova%20Pro-FF9900.svg?logo=amazon-aws&logoColor=white)](https://aws.amazon.com/bedrock/)
[![SQLAlchemy](https://img.shields.io/badge/SQLAlchemy-2.0-D71F00.svg?logo=sqlalchemy&logoColor=white)](https://www.sqlalchemy.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

An enterprise-grade autonomous Text-to-SQL conversational agent that translates complex business questions into dialect-optimized SQL, validates query execution plans via pre-execution cost guardrails, self-heals runtime errors dynamically, and renders interactive analytical visualizations and natural language summaries.

**[🚀 Launch Live Application](https://query-agent.streamlit.app)** • **[💻 GitHub Repository](https://github.com/kkaustav/query-agent)**

---

## Architecture Overview

The system decouples orchestration, semantic pattern retrieval, query cost governance, execution, and visual reporting into a resilient closed-loop pipeline:

```mermaid
flowchart TD
    subgraph Presentation["Presentation & Interaction Layer (app.py)"]
        UI["Streamlit Interface<br/>Multi-Turn Chat"]
        Visuals["Analytics Engine<br/>Plotly Charts + CSV/Excel Exports"]
    end

    subgraph Intelligence["Agent & RAG Orchestration Layer (agent.py)"]
        Agent["NLQueryAgent<br/>Converse Orchestrator"]
        VectorStore["Dynamic Few-Shot Store<br/>(example_store.py)"]
        Advisor["Index Advisor Registry<br/>(index_advisor.py)"]
    end

    subgraph FoundationModels["AWS Bedrock Runtime Engine"]
        Titan["Amazon Titan Text Embeddings v2<br/>(amazon.titan-embed-text-v2:0)"]
        Nova["Amazon Nova Pro 1.0<br/>(amazon.nova-pro-v1:0)"]
    end

    subgraph DataGovernance["Database Engine & Governance (database.py)"]
        Guardrails["Query Guardrails<br/>DDL/DML Filter & AST Check"]
        PlannerCost["Cost & Plan Inspector<br/>EXPLAIN Cost Analysis"]
        DB[(Target Database<br/>SQLite / PostgreSQL / MySQL)]
    end

    %% User Interaction
    UI -->|"1. Natural Language Prompt"| Agent

    %% Retrieval Augmented Generation
    Agent -->|"2. Retrieve Reference Patterns"| VectorStore
    VectorStore -->|"Vectorize & Cosine Match"| Titan
    Titan -->|"Top-K Reference Vectors"| VectorStore
    VectorStore -->|"3. Top-K Syntax Examples"| Agent

    %% Initial SQL Synthesis
    Agent -->|"4. Converse API (Prompt + Context + Schema)"| Nova
    Nova -->|"5. Synthesized Dialect SQL"| Agent

    %% Validation & Cost Inspection
    Agent -->|"6. Inspect SQL"| Guardrails
    Guardrails -->|"Read-Only Verified"| PlannerCost
    PlannerCost -->|"7. EXPLAIN Plan Validation"| DB

    %% Self-Healing Feedback Loop
    PlannerCost -->|"Hazard Detected (Cost / Cartesian Scan)"| Agent
    DB -->|"Execution Error Trace"| Agent
    Agent -.->|"Feedback Loop (Max 3 Retries)"| Nova

    %% Execution & Advisory
    DB -->|"8. Result Set (DataFrame)"| UI
    DB -->|"9. Query Plan Details"| Advisor
    Advisor -.->|"Unindexed Scan Threshold Exceeded"| Nova
    Nova -.->|"Draft Tuning DDL"| Advisor
    Advisor -->|"Tuning Alert & DDL"| UI

    %% Insight Synthesis
    UI -->|"Generate Summary"| Nova
    Nova -->|"Natural Language Insight"| UI
    UI --> Visuals
```

---

## Key System Features

### 1. Amazon Nova Pro Reasoning Engine
* Powered by Bedrock's flagship model `amazon.nova-pro-v1:0` using the structured **Converse API**.
* Context-aware multi-turn reasoning that adapts previous queries based on conversational refinements (e.g., *"Break that down by quarter"* or *"Filter that to Europe only"*).
* Multi-dialect SQL translation supporting syntax rules for **SQLite**, **PostgreSQL**, and **MySQL**.

### 2. Dynamic Few-Shot In-Context Retrieval (RAG)
* Eliminates prompt token bloat by dynamically vectorizing verified schema examples using **Amazon Titan Text Embeddings v2** (`amazon.titan-embed-text-v2:0`).
* Calculates in-memory cosine similarity against golden SQL templates to ground LLM outputs in accurate syntax patterns for complex joins, aggregations, and window functions.

### 3. Pre-Execution Cost Guardrails & Sanitization
* **Zero-Trust SQL Sanitization:** Strips unauthorized multi-statement executions and rejects destructive operations (`DROP`, `DELETE`, `UPDATE`, `INSERT`, `ALTER`, `TRUNCATE`, `GRANT`).
* **Pre-Execution `EXPLAIN` Cost Inspector:**
  * **PostgreSQL:** Evaluates `EXPLAIN (FORMAT JSON)` total planner cost against a safety ceiling (limit: `25,000.0`).
  * **MySQL:** Parses JSON execution plans to check optimizer `query_cost` (limit: `1,000.0`).
  * **SQLite:** Monitors `EXPLAIN QUERY PLAN` output for multiple concurrent unindexed full-table scans to abort accidental Cartesian products before execution.

### 4. Autonomous Self-Healing Reflection Loop
* When a query encounters execution timeouts, syntax mistakes, or planner rejections, the agent initiates an automated reflection cycle (up to 3 attempts).
* The raw database exception and execution trace are packaged into a structured correction prompt, guiding Nova Pro to repair the query without manual intervention.

### 5. Automated Database Tuning & Index Advisor
* Background registry tracks repeated full-table scans (`SCAN TABLE`) across queries.
* Once a table exceeds a configurable scan frequency threshold, Nova Pro evaluates table schemas alongside recent query patterns to draft index creation DDL statements (e.g., `CREATE INDEX idx_orders_customer_id ON orders(customer_id);`).

### 6. Interactive BI Dashboard
* **Dynamic Visualizations:** Plotly bar charts automatically render for dimensional and metric aggregations.
* **Executive Insights:** Generates concise, natural language summaries directly beneath result tables.
* **One-Click Export:** Download query results directly in CSV or formatted Microsoft Excel (`.xlsx`) workbooks.

---

## Repository Structure

```text
query-agent/
├── .streamlit/
│   └── secrets.toml          # AWS credentials & region configuration (git-ignored)
├── data/
│   └── ecommerce.db          # Pre-seeded SQLite relational sandbox
├── agent.py                  # Core Bedrock Converse orchestrator & self-healing loop
├── app.py                    # Streamlit UI, session state, and Plotly visualization
├── database.py               # SQLAlchemy multi-dialect engine, cost inspector & hooks
├── example_store.py          # Dynamic few-shot RAG via Titan Text Embeddings v2
├── index_advisor.py          # Full-table scan tracker and automated DDL advisor
├── seed_data.py              # Automated schema generation and relational data seed
├── requirements.txt          # Production dependencies
├── .gitignore                # Environment and secrets protection
└── README.md                 # Project architecture documentation
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
* An AWS Account with Amazon Bedrock model access granted for:
  * `amazon.nova-pro-v1:0`
  * `amazon.titan-embed-text-v2:0`

### 1. Clone & Set Up Virtual Environment
```bash
git clone [https://github.com/kkaustav/query-agent.git](https://github.com/kkaustav/query-agent.git)
cd query-agent

python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Configure AWS Credentials
Ensure your environment is authenticated via the AWS CLI:
```bash
aws configure
```
Alternatively, configure local credentials in `.streamlit/secrets.toml`:
```toml
AWS_ACCESS_KEY_ID = "AKIAXXXXXXXXXXXXXXXX"
AWS_SECRET_ACCESS_KEY = "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
AWS_DEFAULT_REGION = "us-east-1"
```

### 3. Seed the Sandbox Database
```bash
python seed_data.py
```

### 4. Launch the Streamlit Dashboard
```bash
streamlit run app.py
```
Access the application at `http://localhost:8501`.

---

## Cloud Deployment (Streamlit Community Cloud)

1. Fork or push this repository to GitHub.
2. Navigate to **[share.streamlit.io](https://share.streamlit.io)** and select **Deploy a public app from GitHub**.
3. Set the deployment fields:
   * **Repository:** `kkaustav/query-agent`
   * **Branch:** `main`
   * **Main file path:** `app.py`
4. Expand **Advanced settings...** ➔ **Secrets**, then add your AWS Bedrock IAM credentials:
   ```toml
   AWS_ACCESS_KEY_ID = "AKIAXXXXXXXXXXXXXXXX"
   AWS_SECRET_ACCESS_KEY = "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
   AWS_DEFAULT_REGION = "us-east-1"
   ```
5. Click **Deploy!**.

---

## Security & Execution Governance

* **Isolated Secret Storage:** Credentials are never exposed to the frontend client. In production, AWS keys reside entirely within encrypted runtime secrets (`st.secrets`).
* **Multi-Dialect Timeout Enforcement:**
  * **SQLite:** Enforced via custom C-extension VM opcode progress handlers (`set_progress_handler`) optimized for Python 3.12+.
  * **PostgreSQL:** Configured with `SET statement_timeout = 5000;`.
  * **MySQL:** Enforced with `SET SESSION max_execution_time = 5000;`.
* **Read-Only Transaction Isolation:** PostgreSQL connections issue `SET TRANSACTION READ ONLY;` prior to statement execution to eliminate mutation vectors.

---

## License

This project is licensed under the MIT License — see the [LICENSE](LICENSE) file for details.