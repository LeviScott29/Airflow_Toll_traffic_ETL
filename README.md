# Toll Traffic ETL Pipeline with Apache Airflow

An automated ETL pipeline built with Apache Airflow that extracts toll traffic data from multiple file formats, performs transformations to create a unified dataset, and loads the processed data for analytics.

The pipeline demonstrates building a **data ingestion and transformation workflow using Airflow DAG orchestration and Python-based processing**.

---

# Features

- Extracts data from multiple file formats:
  - CSV
  - TSV
  - Fixed-width text files
- Performs column extraction and transformation
- Consolidates datasets into a single structured file
- Automates workflow orchestration with Apache Airflow DAGs
- Supports repeatable batch processing of toll traffic data

---
# Architecture Overview

Pipeline workflow:
```
Source Files
(CSV / TSV / Fixed Width)
│
▼
Extract Required Columns
│
▼
Merge Data Sources
│
▼
Transform Dataset
│
▼
Load Final Dataset
```

---

# DAG Workflow

The Airflow DAG orchestrates several tasks:

1. **unzip_data**
   - Extract compressed source data

2. **extract_data_from_csv**
   - Extract relevant fields from vehicle CSV file

3. **extract_data_from_tsv**
   - Extract fields from toll plaza TSV file

4. **extract_data_from_fixed_width**
   - Extract payment data from fixed-width text file

5. **consolidate_data**
   - Combine extracted datasets into a single dataset

6. **transform_data**
   - Apply formatting and transformations to the consolidated dataset

7. **load_data**
   - Save final processed data for downstream analysis

Task dependency flow:
```
unzip_data
│
├── extract_csv
├── extract_tsv
└── extract_fixed_width
│
▼
consolidate_data
│
▼
transform_data
│
▼
load_data
```
- Python
- Apache Airflow
- Bash scripting
- CSV / TSV / Fixed-width file processing
