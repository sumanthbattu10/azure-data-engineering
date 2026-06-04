# Azure Data Engineering Pipeline

End-to-end data engineering solutions using Microsoft Azure — 
Azure Data Factory, Azure Databricks, Azure Data Lake Storage Gen2,
Event Hub, Stream Analytics, and Azure DevOps CI/CD.

## Tech Stack
- **Azure Data Factory** — ETL/ELT pipelines, linked services, triggers
- **Azure Databricks** — Spark/PySpark processing, Delta Lake
- **Azure Data Lake Storage Gen2** — data lake design, RBAC
- **Azure Event Hub** — real-time streaming ingestion
- **Azure Stream Analytics** — real-time query processing
- **Azure DevOps** — CI/CD pipelines, automated deployment
- **Python** — data processing, automation, Azure SDK
- **SQL** — advanced queries, data modeling, optimization
- **Power BI** — dashboards and executive reporting
- **Terraform** — infrastructure as code

## Architecture
Raw Data Sources → Azure Event Hub (streaming) →
Azure Stream Analytics → Azure Data Lake Storage Gen2 →
Azure Data Factory (batch ETL) → Azure Databricks (PySpark) →
Delta Lake → Azure Synapse Analytics → Power BI Dashboard

## Projects

### 1. Azure Data Factory ETL Pipeline
- Linked services connecting 8+ data sources
- Mapping data flows with transformation logic
- Triggers and scheduling for automated runs
- Integration runtime for on-premises connectivity

### 2. Azure Databricks Spark Processing
- PySpark jobs with Delta Lake ACID transactions
- Medallion Architecture (Bronze/Silver/Gold)
- Unity Catalog for data governance
- MLflow for experiment tracking

### 3. Real-Time Streaming Pipeline
- Azure Event Hub ingestion with consumer groups
- Azure Stream Analytics windowed aggregations
- Real-time alerting and anomaly detection
- Exactly-once processing semantics

### 4. Azure DevOps CI/CD
- Automated build and deployment pipelines
- Infrastructure as code with Terraform
- Automated testing and quality gates
- Blue-green deployment strategies

## Results
- Reduced data latency from 6 hours to under 30 minutes
- Improved pipeline throughput by 35%
- Zero data quality escapes over 3 consecutive months
- Reduced deployment time from 6 hours to 30 minutes
- 99.7% pipeline availability
