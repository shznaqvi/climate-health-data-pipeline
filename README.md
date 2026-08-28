# Climate-Health Research Data Ingestion Engine
### Apache Airflow | PySpark | REST Web APIs | Pandas | SQL Server | Docker
[![Version](https://img.shields.io/badge/version-1.0.0-blue.svg)](https://github.com/YOUR-USERNAME/climate-health-data-pipeline)
[![Airflow](https://img.shields.io/badge/Apache%20Airflow-2.x-orange.svg)](https://airflow.apache.org/)
[![PySpark](https://img.shields.io/badge/PySpark-Enabled-yellow.svg)](https://spark.apache.org/docs/latest/api/python/)
[![Python](https://img.shields.io/badge/python-3.10-blue.svg)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](https://opensource.org/licenses/MIT)

An enterprise-grade, fault-tolerant ETL pipeline built to orchestrate, clean, validate, and load high-throughput climate-health telemetry data. The engine seamlessly processes both live REST Web API payloads and batch-processed field sensor logsheets (`.csv`, `.xlsx`), implementing automated row-level quarantine and pre-database deduplication to protect relational data integrity.


## 📊 Pipeline Orchestration & DAG Workflows

### 1. Ingestion Engine Overview
<p align="center">
  <img src="dags/docs/images/airflow_data_ingestion_DAGS.jpg" alt="Airflow Data Ingestion DAGs Overview" width="100%">
  <br>
  <sub><b>Figure 1:</b> Apache Airflow dashboard displaying active multi-source ETL DAG schedules and execution metrics.</sub>
</p>

---

### 2. Sensor Logsheet Processing Workflow (`TempU-03 / SHAPES`)
<p align="center">
  <img src="dags/docs/images/airflow_climate_data_DAG_Tempu03.jpg" alt="Climate Logsheet ETL DAG Execution" width="100%">
  <br>
  <sub><b>Figure 2:</b> Task graph executing CSV logsheet parsing, timeline overlap auditing, SQL Server deduplication, and row-level quarantine exports.</sub>
</p>

---

### 3. REST Web API Telemetry Ingestion (`Fitbit API`)
<p align="center">
  <img src="dags/docs/images/airflow_fitbit_api_data_DAG.jpg" alt="Fitbit REST API Data Ingestion DAG" width="100%">
  <br>
  <sub><b>Figure 3:</b> Automated API fetch DAG managing bearer token authentication, rate-limited pagination, and direct SQL target ingestion.</sub>
</p>
