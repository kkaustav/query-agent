# Autonomous Natural Language Database Query Agent & FinOps Copilot (Serverless Data Lakehouse)

[![Streamlit App](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://query-agent.streamlit.app/)
[![AWS ECS Live](https://img.shields.io/badge/AWS%20ECS-Fargate%20Live-orange?logo=amazon-aws)](https://qu-ad51b077a15a4aada7d7d6906df94105.ecs.us-east-1.on.aws/)
[![Python 3.12](https://img.shields.io/badge/python-3.12-blue.svg)](https://www.python.org/downloads/)
[![Amazon Bedrock](https://img.shields.io/badge/Amazon%20Bedrock-Nova%20Pro-792EE5)](https://aws.amazon.com/bedrock/)
[![Amazon Athena](https://img.shields.io/badge/Amazon%20Athena-Serverless%20Trino-232F3E)](https://aws.amazon.com/athena/)
[![Amazon S3](https://img.shields.io/badge/Amazon%20S3-Data%20Lake-569A31)](https://aws.amazon.com/s3/)
[![AWS Glue](https://img.shields.io/badge/AWS%20Glue-Data%20Catalog-blue)](https://aws.amazon.com/glue/)
[![Amazon DynamoDB](https://img.shields.io/badge/Amazon%20DynamoDB-Distributed%20Locks-4053D6)](https://aws.amazon.com/dynamodb/)
[![FinOps Cloud Cost Intelligence](https://img.shields.io/badge/FinOps-Cloud%20Cost%20Intelligence-00C49F)](https://www.finops.org/)
[![Docker](https://img.shields.io/badge/Docker-Containerized-2496ED?logo=docker&logoColor=white)](https://www.docker.com/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

> **From Data to Decisions — Without Writing SQL.**
>
> *Also known as the **NL BI FinOps Agent**.*

*Live Deployments:*
- **Primary (Always Online):** [https://query-agent.streamlit.app/](https://query-agent.streamlit.app/)
- **AWS Deployment (ECS Fargate + ALB):** [https://qu-ad51b077a15a4aada7d7d6906df94105.ecs.us-east-1.on.aws/](https://qu-ad51b077a15a4aada7d7d6906df94105.ecs.us-east-1.on.aws/) *(Note: The AWS ECS Fargate task is scaled to 0 tasks when idle to optimize cloud costs; spin up to 1 task on-demand via the AWS CLI — see [On-Demand Service Control](#on-demand-service-control-cost-optimization)).*

A serverless Text-to-SQL conversational analytics application and **FinOps copilot** built on an AWS data lakehouse. Users ask questions in plain English; the application translates them into Trino/Presto SQL, grounds the model in live AWS Glue schemas and a declarative business-metric layer (`metrics.yaml`), validates every query with Abstract Syntax Tree (AST) parsing so that only a single column-pruned, read-only `SELECT` runs, executes it on Amazon Athena over Amazon S3, and returns the generated SQL, an exportable result table, and an interactive Plotly visualization. For AWS cost questions it adds a three-tier FinOps narrative — spend & trend analysis, potential root causes, and prescriptive optimization recommendations.

Containerized with Docker, published to **Amazon ECR**, and hosted on **Amazon ECS on AWS Fargate** with managed ALB routing and keyless IAM Task Role authentication for **Amazon Bedrock**.

---

## The Business Problem: Data Is Available, Insight Is Not

Businesses often have the data they need but not an easy way to turn it into answers. Consider a monthly AWS Cost and Usage Report (CUR): a finance manager may simply want to know *why the bill increased, which services drove the change, and what can be done about it*. Getting there traditionally spans several tools, skill sets and hand-offs:

```text
S3 (store CUR files) → Glue (catalog tables) → Athena (write SQL) → BI tool (build visuals) → FinOps (interpret & act)
```

- **Tool configuration:** teams must connect and configure storage, metadata, query and visualization services (for example, an S3 → Glue → Athena → QuickSight pipeline with CUDOS dashboards).
- **SQL and BI dependency:** every new question may require new SQL or a reconfigured chart or dashboard.
- **Interpretation gap:** a chart shows *what* happened, not necessarily *why* it happened or *what to do next*.
- **Slow hand-offs:** non-technical stakeholders wait on engineering or analytics teams.

## The Solution: One Interface, One Request

This application is an intelligent layer over the data platform. It does **not** replace S3, Glue or Athena — it orchestrates them and hides much of their technical complexity from the person asking the question.

| Step | What happens |
| :--- | :--- |
| **1. Upload** | Add a CUR file or another supported dataset (validated & cut over atomically). |
| **2. Ask** | Pose a question in plain English. |
| **3. Analyze** | The application generates, AST-validates and runs the SQL under strict scan limits. |
| **4. Understand** | Review the result table and interactive chart. |
| **5. Act** | Read the FinOps narrative: candidate causes to investigate and optimization actions to consider. |

**Example request:** *"My AWS bill increased significantly. Identify the top five services driving the increase, show the trend, explain possible causes and recommend ways to optimize spending."*

The result is not just a chatbot reply but a **traceable analytical workflow**: the generated SQL, the result table and the visualization behind the explanation stay visible for review.

**What makes it different**
- **Accessibility:** business users ask questions in everyday language instead of learning SQL.
- **End-to-end workflow:** distributed ingestion locking, staging validation, schema grounding, query generation, AST validation, execution and presentation are orchestrated in one application.
- **Decision support:** cost answers go beyond totals to suggest possible drivers and optimization actions.
- **Governance:** generated SQL is validated as read-only with `SELECT *` projection blocking, and enforced Athena workgroup limits (100 MB per query) cap scan cost.

## Who It's For

| User | Value provided |
| :--- | :--- |
| **Finance Manager** | Understand service-level spend, budget changes and cost drivers without writing SQL. |
| **FinOps Analyst** | Investigate anomalies, identify optimization opportunities and prepare evidence-backed recommendations. |
| **Cloud Architect / Engineer** | Inspect resource and usage patterns, query details and technical optimization opportunities. |
| **Business Analyst** | Explore datasets, create tables and visualizations, and answer ad hoc questions more independently. |
| **Engineering Manager** | Prioritize cost work using trends and potential impact areas. |
| **Startup Founder** | Get a clearer view of cloud spend without building an analytics workflow for every question. |

The application is not intended to replace the judgment of finance, FinOps or engineering professionals. It aims to make the first layer of analysis faster and more accessible, while keeping the query and result visible for review.

---

## 🖼️ FinOps Copilot in Action

### 1. Anomaly Detection & 3-Tier Diagnostics
Asked *"Find the top anomalies and provide me recommendations to resolve it"*, the application detects statistical cost outliers in the synthetic CUR (Aug/Sep 2026) and ranks ten AWS product codes by anomalous spend. The largest are **$19,196.32 in Amazon EC2** and **$3,900.10 in Compute Savings Plans**; together they account for **78.5%** of the **$29,408** of anomalous spend isolated in this case study. The response then lists candidate root causes for each service and an actionable playbook for AWS cost reduction.

### 2. Daily Spend Trend & Spike Analysis
Asked for September 2026 daily spend and its biggest spikes, the application returns a 30-row result and a time-series chart. **September 1 stands out at $12,724.09 — 451.72% above the average day** — and the narrative lists month-start commitment-fee amortization among the candidate causes to investigate.

### 3. Analytical Data Tables & Interactive Visualization
The platform leverages Athena's serverless query engine to isolate and analyze anomalous cloud spend across various AWS product codes. The output is presented via interactive Plotly charts and comprehensive data grids, complete with one-click export options for CSV and Excel.

> These are demonstration results from a synthetic CUR dataset — not production customer findings or verified savings.

---

## Architecture Overview

The platform decouples streaming data ingestion, distributed lock coordination, semantic vector retrieval, AST query sanitation, serverless query execution, and conversational FinOps business intelligence:

```text
[ User Browser / Client ]
           │
           ▼
[ Application Load Balancer (Managed HTTPS) ]
           │
           ▼
[ AWS ECS on AWS Fargate (Streamlit Presentation Layer) ]
     │                               │
     ├─► [ Amazon Bedrock ]          ├─► [ AST SQL Sanitizer (sqlglot) ]
     │     ├─ Nova Pro 1.0           │     ├─ Strict Read-Only SELECT Check
     │     └─ Titan Embeddings v2    │     └─ SELECT * Projection Blocker
     │                               │
     ├─► [ metrics.yaml ]            ├─► [ Amazon Athena (Serverless Trino) ]
     │     └─ Semantic Metric Store  │     └─ Enforced 100 MB Cutoff & 30s Timeout
     │                               │
     └─► [ Amazon DynamoDB ]         ├─► [ AWS Glue Data Catalog ]
           └─ Distributed Ingestion  │     └─ Atomic VersionId Cutover (query_agent_db)
              Locks & Fencing        │
                                     └─► [ Amazon S3 Lakehouse ]
                                           ├─ Versioned Data Prefixes (/data/)
                                           ├─ Durable State Manifests (/_ingestion_state/)
                                           └─ Athena Query Results (/athena-results/)
```

### Core Components

| Layer | Component | Responsibility |
| :--- | :--- | :--- |
| **Presentation** | Streamlit on ECS Fargate | Chat, uploads, result tables, Plotly charts and exports. |
| **AI reasoning** | Amazon Bedrock Nova Pro | Natural-language understanding, SQL generation and FinOps narrative. |
| **Semantic grounding** | `metrics.yaml` + Titan Text Embeddings v2 | Metric definitions, synonyms, default filters, schema context and relevant few-shot examples. |
| **Safety & Cost Control** | `sqlglot` AST validation | Allows only single read-only `SELECT` queries; blocks `SELECT *` projections and destructive or multi-statement SQL. |
| **Query engine** | Amazon Athena (Trino) | Executes validated queries over S3 with fail-closed 100 MB scan and 30s timeout limits. |
| **Distributed coordination** | Amazon DynamoDB (`nl_bi_finops_ingestion_locks`) | Coordinates cross-process table leases, background heartbeats and monotonic fencing tokens during ingestion and cleanup. |
| **Metadata** | AWS Glue Data Catalog | Provides table schemas and supports atomic `UpdateTable` cutover with `VersionId` optimistic concurrency control. |
| **Storage** | Amazon S3 data lakehouse | Stores versioned dataset prefixes, durable ingestion state manifests and Athena query outputs. |

### How a Natural-Language Request Is Processed

1. **Ask** — The user enters a question in the Streamlit chat.
2. **Ground** — The agent retrieves Glue table schemas, metric formulas, synonyms, default filters and relevant examples.
3. **Generate** — Bedrock Nova Pro writes Trino SQL and infers joins across tables.
4. **Sanitize** — `sqlglot` parses the query; only a single column-pruned, read-only `SELECT` is allowed through.
5. **Execute** — Athena runs the query over S3 within enforced workgroup scan (100 MB) and timeout (30s) limits.
6. **Self-heal** — If a query fails with a retryable SQL or projection error, the trace is returned to the model for up to three corrective attempts.
7. **Explain** — The app returns the result table (CSV/Excel export), an interactive chart and, for cost questions, a FinOps narrative.

---

## Key System Features

### 1. Cloud FinOps Copilot & Cost Optimization Recommendations
- **AWS CUR Ingestion & Schema Alignment:** Native understanding of AWS Cost and Usage Reports (CUR), dynamically mapping unblended/effective costs, usage types, operations, and pricing models (`On-Demand`, `Savings Plans`, `Spot`, `Reserved Instances`).
- **3-Tier Executive FinOps Diagnostics:** Cost and infrastructure inquiries are answered in three tiers:
  1. 📊 **Spend & Trend Analysis:** Concrete dollar totals, MoM variances, daily trends, service-level spend, and percentage spikes.
  2. 🔍 **Root Cause Diagnosis (Hypotheses):** Candidate technical drivers inferred from aggregated data (e.g., month-start recurring commitment fees, tax grouping, unamortized RIs/commitments, oversized dev/test compute, or month-end batch pipelines). These are plausible hypotheses to investigate, **not verified findings**.
  3. 💡 **Prescriptive Cost Optimization:** Suggested AWS architectural and procurement actions:
     - **Immediate Waste Elimination:** Reviewing idle or unattached resources (e.g., EBS volumes), scheduling non-production workloads (e.g., AWS Instance Scheduler), and cleaning up obsolete snapshots where appropriate.
     - **Commitment & Pricing:** Reviewing Compute and EC2 Instance Savings Plans coverage; assessing Spot for fault-tolerant workloads.
     - **Modernization & Rightsizing:** Using AWS Compute Optimizer as an input to rightsizing, evaluating AWS Graviton processors, and reviewing GP2-to-GP3 EBS opportunities.
     - **Governance:** Improving ownership and environment tagging; considering AWS Cost Anomaly Detection alerts (e.g., via SNS) and tag-or-terminate policies with suitable controls.
- **Recommendation Boundary:** Treat every recommendation as an investigation or optimization *opportunity* until a drill-down query, resource review, or engineering validation confirms it.

### 2. Business Semantic Metric Layer (`metrics.yaml`)
- Targets a primary failure mode of Text-to-SQL: business definition ambiguity.
- Declaratively maps business formulas (e.g., `net_revenue = SUM(order_value - discount_amount)`), entity synonyms (e.g., `"Prime members"` -> `is_prime = true`), and default filter predicates (e.g., `order_status = 'delivered'`).
- Injects ground-truth formulas directly into Bedrock Nova Pro reasoning prompts, reducing ad-hoc metric hallucinations.

### 3. Serverless Lakehouse & Atomic Staged Ingestion (`athena_manager.py`)
- Executes distributed SQL across tabular datasets (`CSV`, `TSV`, `Parquet`, `XLSX`, `XLS`) using Amazon Athena's serverless Trino/Presto engine.
- **Distributed Lease Locking & Fencing:** Uses Amazon DynamoDB (`nl_bi_finops_ingestion_locks`) with background lease heartbeats and an atomic global counter (`GLOBAL_FENCING_COUNTER`) to prevent concurrent upload or cleanup race conditions across multiple ECS tasks/processes.
- **Full-Dataset Staging Validation:** Uploads land in isolated S3 prefixes (`data/<table>/ingestion_id=<uuid>/`) and are validated via an Athena staging table before cutover—enforcing readable rows, valid numeric aggregates (no `NaN`/`Infinity`), and a strict **0-null policy** on mandatory AWS CUR columns (`CUR_COLUMN_TYPE_OVERRIDES`).
- **Atomic Glue Cutover & Verified Rollback:** Replaces existing tables atomically via AWS Glue `UpdateTable` with `VersionId` optimistic concurrency fencing, verifies the updated S3 location and schema, and automatically rolls back to the prior snapshot on failure.
- **Retired Prefix Retention:** Previous production S3 prefixes are marked `RETIRED` in durable S3 manifests and retained for a 1-hour grace period (`min_age_seconds=3600`) so in-flight Athena queries never fail during table replacement.

### 4. Dual-Track Data Ingestion Strategy
- **Ad-Hoc UI Streaming (In-Band):** Streamlit file uploader supports batch uploading 10+ heterogeneous datasets (CSV, TSV, XLSX, XLS, PARQUET up to 1 GB per file; 50 MB / 100k rows cap for Excel conversion) utilizing memory-buffered streaming to prevent container Out-Of-Memory (OOM) crashes on 2 GB Fargate tasks.
- **Lakehouse Scale Ingestion (Out-Of-Band):** For multi-gigabyte or terabyte workloads exceeding container ephemeral storage (default 20 GB), files bypass the web server via S3 Multipart Uploads or AWS CLI direct-to-S3 ingestion, followed by immediate AWS Glue Catalog schema synchronization.

### 5. AST Query Sanitization, Projection Pruning & Read-Only Safety (`database.py`)
- Employs Abstract Syntax Tree (AST) analysis via `sqlglot` to parse generated SQL before transmission to Athena.
- **Strict Read-Only Enforcement:** Allows only a single read-only `SELECT` through; traverses the expression tree to reject destructive or administrative operations (`DROP`, `DELETE`, `UPDATE`, `INSERT`, `ALTER`, `CREATE`, `TRUNCATE`, `GRANT`, `REVOKE`, `CALL`, `SET`, `USE`) and multi-statement SQL.
- **Cost-Aware Projection Guardrail:** Blocks `SELECT *` and `SELECT table.*` projections (`ProjectionCostExceededError`), prompting the agent to select only the specific columns needed for the user's question.
- **Benchmark result:** all destructive attempts in the 50-prompt test set were blocked.

### 6. Cross-Table Relational Join Intelligence
- Bedrock Nova Pro inspects all active tables in the AWS Glue Data Catalog (`query_agent_db`) simultaneously.
- Infers implicit foreign-key relationships across disparate operational domains (e.g., joining customer demographic attributes from `zomato_customers` with transaction volume in `zomato_orders`).
- Employs explicit Presto/Trino SQL join semantics (`LEFT JOIN`, `INNER JOIN`) to reduce `COLUMN_NOT_FOUND` runtime exceptions.

### 7. Autonomous Self-Healing Execution Loop
- Intercepts retryable SQL syntax errors, projection guardrail rejections (`ProjectionCostExceededError`), schema mismatches, and Trino dialect errors in real time while failing closed on non-retryable infrastructure or scan-limit errors.
- Feeds database exceptions and execution traces back into Nova Pro for up to 3 automated correction iterations before returning output.
- **Benchmark result:** self-healing recovered 3 of the 4 first-pass failures, lifting overall success from 92.0% to 98.0%.

### 8. Partition & Storage Layout Advisor (`index_advisor.py`)
- Analyzes recurring query `WHERE` filters, `JOIN` predicates, and aggregations to output recommendations for:
  - **Partition Keys:** Identifying high-cardinality pruning candidates (e.g., `order_date`, `city`, `season`).
  - **Columnar Formats:** Advising conversion from raw CSV/JSON to Apache Parquet with Snappy compression to reduce scanned byte volume (an estimated 80–90% for suitable workloads; not measured in the 50-prompt benchmark).
  - **Bucketing Strategies:** Recommending hash-bucketing keys for frequently joined foreign keys.

### 9. Isolated Multi-Tab Chat History
- Session management powered by URL query parameters (`?session=<session_id>`) and a shared application resource cache.
- Opening historical queries renders selected conversations in a new, independent browser tab without disrupting the active query session.
- Sidebar controls include **Inspect Athena / Glue Schema** and **New Chat / Reset**.

### 10. Transparent, Reviewable Results
- Each answer is accompanied by the generated SQL, a result table with one-click **CSV / Excel export**, and an interactive **Plotly** chart, so users and reviewers can see how an answer was produced.

---

## Security, Guardrails & Governance

### 1. Scoped, Least-Privilege IAM Task Role
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
        "s3:DeleteObject",
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
        "athena:StopQueryExecution",
        "athena:GetWorkGroup"
      ],
      "Resource": "arn:aws:athena:*:*:workgroup/primary"
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
        "glue:UpdateTable",
        "glue:DeleteTable",
        "glue:BatchCreatePartition"
      ],
      "Resource": [
        "arn:aws:glue:*:*:catalog",
        "arn:aws:glue:*:*:database/query_agent_db",
        "arn:aws:glue:*:*:table/query_agent_db/*"
      ]
    },
    {
      "Sid": "DynamoDBIngestionLockAccess",
      "Effect": "Allow",
      "Action": [
        "dynamodb:GetItem",
        "dynamodb:PutItem",
        "dynamodb:UpdateItem",
        "dynamodb:DeleteItem"
      ],
      "Resource": "arn:aws:dynamodb:*:*:table/nl_bi_finops_ingestion_locks"
    }
  ]
}
```

### 2. Athena Cost Governance & Fail-Closed Guardrails
- Amazon Athena charges **$5.00 per TB** of data scanned.
- **Enforced Workgroup Per-Query Limit (100 MB):** The Athena workgroup is configured with `BytesScannedCutoffPerQuery = 104857600` (100 MB) and `EnforceWorkGroupConfiguration = true`. `AthenaManager` verifies this guardrail at startup via `GetWorkGroup` (`strict_workgroup_check=True`) and refuses to execute queries if the cutoff is missing or unverified.
- **Query Timeout Enforcement (30s):** Athena queries automatically cancel after 30 seconds (`query_timeout_sec=30`) via `StopQueryExecution` to mitigate resource exhaustion from non-converging joins.

### 3. Scope of These Controls
These controls reduce risk; they do not remove the need for production identity management, data permissions, audit logging and security review. The documented security model covers the application task role. Multi-user authentication (SSO), per-user and row-level data permissions, audit logs and stronger tenant isolation are on the [roadmap](#roadmap).

---

## Evaluation Benchmark & Verification Suites

### 1. Pre-Production & Live AWS Integration Test Suites
The repository includes two automated verification suites:
- **`test_preproduction_checklist.py`**: Validates 14 SQL/projection test cases and 12 forbidden DDL/DML verbs across dialects, `clean_sql_output()` malformed LLM output handling, S3 partial deletion failure handling, distributed DynamoDB lock contention, 0-null policy enforcement on mandatory CUR columns, Glue `VersionId` stale-worker fencing, and `RETIRED` prefix retention.
- **`live_aws_integration_test.py`**: Runs against live AWS resources in `us-east-1` to verify the enforced 100 MB Athena workgroup cutoff, live DynamoDB monotonic fencing counter (`GLOBAL_FENCING_COUNTER`), multi-process OS lock contention across distinct PIDs, and end-to-end S3 + Glue + Athena staged ingestion, atomic cutover, and query execution.

```bash
python3 test_preproduction_checklist.py
python3 live_aws_integration_test.py
```

### 2. 50-Prompt Benchmark
The agent was evaluated on a 50-prompt benchmark spanning FinOps cloud costs, retail, music streaming, and food delivery datasets:

| Metric | Result | Notes |
| :--- | :--- | :--- |
| **First-Pass SQL Generation Accuracy** | 92.0% (46/50) | Correct on the first attempt. |
| **Post-Self-Healing Success Rate** | 98.0% (49/50) | 3 of the 4 first-pass failures were recovered. |
| **Average Bedrock End-to-End Latency** | 1.42 s | Average end-to-end model latency. |
| **Average Bedrock Token Cost per Query** | $0.0012 | About $1.20 per 1,000 questions, excluding other infrastructure costs. |
| **AST Mutation Interception Rate** | 100% | All destructive attempts in the test set (e.g., `DROP`/`DELETE`/`INSERT`) were blocked. |

> **How to read these numbers:** they come from the project's 50-prompt benchmark on synthetic datasets. They are evidence of feasibility, **not** a guarantee of production performance or an enterprise accuracy study. Production CUR data can include credits, refunds, taxes and more complex amortization patterns, so the benchmark should be expanded with reviewed real-world test cases and per-domain accuracy reporting (see the [roadmap](#roadmap)).

---

## 💰 FinOps Demonstration Queries (Synthetic CUR)

The agent parses AWS billing datasets (such as `synthetic_cur_aug_sep_2026`) and returns both quantitative analytics and FinOps guidance. These are demonstration results on a synthetic CUR, not production customer savings.

| FinOps Domain | Natural Language Query | Generated Output & Visualizations |
| :--- | :--- | :--- |
| **Statistical Outlier Detection** | `Find the top anomalies and provide me recommendations to resolve it` | 3-sigma anomaly breakdown across AWS services, bar chart of abnormal spend, candidate causes, and immediate waste elimination steps. |
| **Daily Trend & Spikes** | `From synthetic_cur_aug_sep_2026, show total daily spend for every day in September 2026 ordered by date. Which days have the highest spikes and what are the FinOps recommendations?` | 30-day interactive Plotly line chart highlighting Sep 1 and Sep 30 anomalies, followed by candidate-cause guidance on tax & fee amortization. |
| **Non-Prod Waste Audit** | `From synthetic_cur_aug_sep_2026, show total spend by product_product_name for the dev and test-qa accounts ordered by total spend descending. Provide rightsizing and scheduling recommendations.` | Categorical cost breakdown across dev/QA environments with scheduled shutdown and Spot instance adoption playbooks. |
| **Tag Hygiene Governance** | `From synthetic_cur_aug_sep_2026, show total spend grouped by line_item_product_code where resource_tags_user_owner is null ordered by total spend descending.` | Unallocated infrastructure cost audit paired with tagging-governance and tag-or-terminate policy recommendations. |
| **Compute Sizing Audit** | `From synthetic_cur_aug_sep_2026, show the top 5 EC2 instance types by total spend.` | Instance family cost concentration chart with AWS Graviton migration and Savings Plan recommendations. |

---

## Current Scope, Known Limitations & Roadmap

A credible project separates what is implemented from what is proposed.

### Implemented & Demonstrated
- Natural-language questions translated into Trino SQL, with schema grounding and semantic metric definitions.
- SQL AST validation that allows only a single column-pruned, read-only `SELECT` statement.
- Enforced Athena workgroup scan cutoff (100 MB) and query timeout (30s) verified fail-closed at startup.
- Distributed DynamoDB table locking, full-dataset Athena staging validation, atomic Glue `VersionId` cutover, verified rollback recovery, and `RETIRED` S3 prefix retention.
- Self-healing retries for retryable query errors, up to three corrective iterations.
- Result tables, CSV/Excel export and interactive Plotly charts.
- A FinOps narrative for cost questions: spend analysis, candidate causes and optimization recommendations.

### Known Limitations
- The AWS CUR used for evaluation is **synthetic**, not a live production billing feed.
- Root-cause explanations are **hypotheses** based on aggregated data; the current workflow does not automatically confirm them with follow-up drill-down queries.
- The anomaly method is statistical and does not yet include seasonality-aware forecasting.
- The documented security model covers the application task role; production multi-user authentication and row-level data permissions require additional work.
- The 50-prompt benchmark is indicative and should be expanded.

### Roadmap

| Priority | Enhancement | Expected outcome |
| :---: | :--- | :--- |
| 1 | **Agentic drill-down** | When an anomaly is detected, run follow-up queries by line-item type, pricing term, account and resource to test the suspected cause. |
| 2 | **Quantified savings** | Estimate potential savings where the data and workload context support a defensible calculation. |
| 3 | **Forecast-aware anomalies** | Add seasonality-aware baselines and integrate with AWS Cost Anomaly Detection. |
| 4 | **Larger evaluation harness** | Build a reviewed golden dataset, regression tests and per-domain accuracy reporting. |
| 5 | **Enterprise hardening** | Add SSO, per-user data permissions, audit logs and stronger tenant isolation. |
| 6 | **Production CUR pipeline** | Ingest production CUR data as Parquet partitioned by billing period. |

---

## Sample Showcase Queries

### 1. Metric Aggregation with Default Filters
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
*Evaluation Dataset (synthetic — not a live production billing feed):* [Synthetic CUR Dataset - Aug/Sep 2026](https://drive.google.com/drive/u/0/folders/1VKXuPVwNDiVyaO3pzjG4AGIKEvSi_HZo)
```sql
CREATE EXTERNAL TABLE query_agent_db.synthetic_cur_aug_sep_2026 (
    bill_payer_account_id bigint,
    bill_billing_period_start_date string,
    line_item_usage_account_id string,
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
    reservation_effective_cost double,
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
│   ├── config.toml                     # Streamlit server flags (1 GB maxUploadSize, CORS overrides)
│   └── secrets.toml                    # Local AWS credentials & region configuration (git-ignored)
├── docs/
│   └── images/                         # Architectural visuals & FinOps screenshots
├── agent.py                            # Bedrock Converse orchestrator, FinOps advisor & self-healing loop
├── app.py                              # Streamlit interface, batch streaming ingestion & BI visuals
├── athena_manager.py                   # Distributed DynamoDB locking, staged S3/Glue cutover & Athena executor
├── database.py                         # AST SQL sanitizer (sqlglot), SELECT * blocker & read-only enforcement
├── example_store.py                    # Dynamic few-shot vector store using Titan Embeddings v2
├── index_advisor.py                    # Partition & Storage Layout Advisor for S3 scan optimization
├── metrics.yaml                        # Declarative business metric store & semantic definitions
├── seed_athena.py                      # Automated provisioner for initial S3 data lake & Glue tables
├── test_preproduction_checklist.py     # Unit & failure-injection pre-production test suite
├── live_aws_integration_test.py        # Live AWS multi-process lock & end-to-end integration test suite
├── Dockerfile                          # Production multi-stage build (linux/amd64)
├── .dockerignore                       # Excludes venvs, caches, and secrets from container images
├── requirements.txt                    # Python production dependencies
├── .gitignore                          # Git exclusions
└── README.md                           # System architecture documentation & execution guide
```

---

## Local Development Setup

### Prerequisites
- Python 3.10+ (tested on Python 3.12)
- AWS Account with Bedrock model access (`amazon.nova-pro-v1:0` and `amazon.titan-embed-text-v2:0`)
- Configured AWS CLI profile with Athena, Glue, S3, and DynamoDB permissions

### 1. Clone & Setup Environment
```bash
git clone [https://github.com/kkaustav/query-agent.git](https://github.com/kkaustav/query-agent.git)
cd query-agent

python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Configure AWS Access & Guardrails
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

Ensure the Athena `primary` workgroup enforces the 100 MB per-query scan limit and the DynamoDB ingestion lock table is active:
```bash
aws athena update-work-group \
    --work-group primary \
    --configuration-updates "BytesScannedCutoffPerQuery=104857600,EnforceWorkGroupConfiguration=true" \
    --region us-east-1

aws dynamodb create-table \
    --table-name nl_bi_finops_ingestion_locks \
    --attribute-definitions AttributeName=lock_key,AttributeType=S \
    --key-schema AttributeName=lock_key,KeyType=HASH \
    --billing-mode PAY_PER_REQUEST \
    --region us-east-1
```

### 3. Provision S3 Data Lake, Run Tests & Launch Application
```bash
python seed_athena.py
python test_preproduction_checklist.py
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
To eliminate 24/7 AWS Fargate compute charges when not actively presenting demos or interviewing, scale the container task count to zero. All S3 data lake files, Glue table catalogs, DynamoDB lock tables, and Docker container images remain intact.

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

## Project Identity

- **Project:** Autonomous Natural Language Database Query Agent & FinOps Copilot
- **Author:** Kaustubh Kar
- **Application:** [query-agent.streamlit.app](https://query-agent.streamlit.app/)
- **Source code:** [github.com/kkaustav/query-agent](https://github.com/kkaustav/query-agent) (MIT License)

---

## License

This project is licensed under the MIT License — see the [LICENSE](LICENSE) file for details.
