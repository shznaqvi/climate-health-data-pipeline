# Climate-Health Research Data Ingestion Engine
### Apache Airflow | PySpark | REST Web APIs | Pandas | SQL Server | Docker

An enterprise-grade, fault-tolerant ETL pipeline built to orchestrate, clean, validate, and load high-throughput climate-health telemetry data. The engine seamlessly processes both live REST Web API payloads and batch-processed field sensor logsheets (`.csv`, `.xlsx`), implementing automated row-level quarantine and pre-database deduplication to protect relational data integrity.

---

## 🏗️ System Architecture

```text
                       +-----------------------------------+
                       |   Multi-Source Ingestion Layer    |
                       +-----------------------------------+
                                   |           |
              +--------------------+           +--------------------+
              |                                                     |
  [ REST Web API Endpoints ]                           [ File System / Storage Mounts ]
  • OAuth2 / Bearer Auth                               • Sensor Logsheets (.csv, .xlsx)
  • Rate-Limited Pagination                            • Batch Drop Directory
              |                                                     |
              +--------------------+           +--------------------+
                                   |           |
                                   v           v
                       +-----------------------------------+
                       |      Apache Airflow DAG Engine    |
                       |     (climate_health_etl_dag)      |
                       +-----------------------------------+
                                       |
                                       v
                       +-----------------------------------+
                       |   PySpark / Pandas Data Cleaning   |
                       |  & Schema Standardization Module  |
                       +-----------------------------------+
                                       |
                                       v
                       +-----------------------------------+
                       |    Two-Tier Data Integrity Gate   |
                       +-----------------------------------+
                         /                               \
       [ Clean & Validated Records ]               [ Non-Conforming Records ]
                     |                                       |
                     v                                       v
        +-------------------------+             +-------------------------+
        |  SQL Server Pre-Filter  |             | Row-Level Quarantine    |
        |  & Bulk Ingestion       |             | Output (.json & .csv)   |
        +-------------------------+             +-------------------------+
                     |
                     v
        +-------------------------+
        |  dbo.etl_execution_logs |
        |   (Audit Trail Table)   |
        +-------------------------+
