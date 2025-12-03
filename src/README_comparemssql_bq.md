# MSSQL to BigQuery Data Validation Tool

This document describes how to use the `comparemssql_bq.py` script to validate data integrity between MSSQL source and BigQuery destination using a connection string approach.

## Overview

The `comparemssql_bq.py` script provides comprehensive validation of data transferred from MSSQL to BigQuery. It checks both completeness (row counts) and correctness (schema and sample data) to ensure data integrity during the ETL process.

## Prerequisites

- Python 3.7+
- Required Python packages: `pandas`, `google-cloud-bigquery`, `pyodbc` (or appropriate MSSQL driver)
- Access to Google Cloud project with BigQuery API enabled
- Access to MSSQL database
- Appropriate credentials for both systems
- `.env` file with SQL_CONNECTION_STRING environment variable (optional)

## Installation

1. Ensure all dependencies are installed:
   ```
   pip install pandas google-cloud-bigquery pyodbc
   ```

2. Make sure the `mssql_client.py` and `bigquery_client.py` modules are available in the same directory as `comparemssql_bq.py` or installed as Python packages.
3. Set up your environment variables, either in a `.env` file or in your shell:
   ```
   # In .env file
   SQL_CONNECTION_STRING=DRIVER={ODBC Driver 17 for SQL Server};SERVER=your-server;DATABASE=your-db;UID=your-username;PWD=your-password;
   ```
   Or export them in your shell:
   ```bash
   export SQL_CONNECTION_STRING="DRIVER={ODBC Driver 17 for SQL Server};SERVER=your-server;DATABASE=your-db;UID=your-username;PWD=your-password;"
   ```

## Usage

### Basic Usage

```bash
# Using SQL_CONNECTION_STRING from environment (recommended)
python comparemssql_bq.py \
  --bq-project-id your-gcp-project \
  --bq-dataset your_bigquery_dataset

# Or passing connection string directly
python comparemssql_bq.py \
  --bq-project-id your-gcp-project \
  --bq-dataset your_bigquery_dataset \
  --mssql-connection-string "DRIVER={ODBC Driver 17 for SQL Server};SERVER=your-server;DATABASE=your-db;UID=your-username;PWD=your-password;"
```

### Advanced Usage

```bash
# Validate specific tables only
python comparemssql_bq.py \
  --bq-project-id your-gcp-project \
  --bq-dataset your_bigquery_dataset \
  --tables table1 table2 table3

```bash
# Validate only completeness (row counts)
python comparemssql_bq.py \
  --bq-project-id your-gcp-project \
  --bq-dataset your_bigquery_dataset \
  --validation-type completeness

```bash
# Validate only correctness (schema and sample data)
python comparemssql_bq.py \
  --bq-project-id your-gcp-project \
  --bq-dataset your_bigquery_dataset \
  --validation-type correctness

```bash
# Save report to file
python comparemssql_bq.py \
  --bq-project-id your-gcp-project \
  --bq-dataset your_bigquery_dataset \
  --output-file validation_report.txt

```bash
# Custom sample size for correctness validation
python comparemssql_bq.py \
  --bq-project-id your-gcp-project \
  --bq-dataset your_bigquery_dataset \
  --sample-size 500
```

## Command Line Arguments

| Argument | Required | Description |
|----------|----------|-------------|
| `--bq-project-id` | Yes | Google Cloud project ID for BigQuery |
| `--bq-dataset` | Yes | BigQuery dataset name to validate |
| `--mssql-connection-string` | No | MSSQL connection string (overrides SQL_CONNECTION_STRING env var) |
| `--tables` | No | Specific tables to validate (space-separated list) |
| `--sample-size` | No | Number of rows to sample for validation (default: 100) |
| `--output-file` | No | File path to save the validation report |
| `--validation-type` | No | Type of validation: completeness, correctness, or both (default: both) |

## Validation Reports

The script generates detailed validation reports for:

### Completeness Validation
- Row count comparison between MSSQL and BigQuery
- File processing verification
- Detailed reporting of any discrepancies
- Total row counts across all tables

### Correctness Validation
- Schema validation between MSSQL and BigQuery
- Sample data comparison (configurable sample size)
- Data type consistency checking
- Column name and order verification

## Exit Codes

- `0`: All validations passed successfully
- `1`: One or more validations failed

## Examples of Report Output

### Completeness Report Example
```
=== COMPLETENESS VALIDATION REPORT ===
Validation Status: success
Message: MSSQL completeness validation completed

--- Details ---

Total files processed: 5
All files processed successfully: true
Total MSSQL rows: 15432
Total BigQuery rows: 15432

--- File Results ---

File: table1
Table: table1
Status: success
CSV rows: 5432
BigQuery rows: 5432
Rows match: true

File: table2
Table: table2
Status: success
CSV rows: 10000
BigQuery rows: 10000
Rows match: true
```

### Correctness Report Example
```
=== CORRECTNESS VALIDATION REPORT ===
Validation Status: warning
Message: Some tables have validation issues

--- Details ---

Total tables processed: 2
All tables passed validation: false

--- Table Results ---

Table: table1
Status: success
Schema match: true
Sample match: true

Table: table2
Status: failed
Schema match: false
Sample match: true
```

## Integration with CI/CD

The script can be integrated into CI/CD pipelines for automated validation:

```bash
# In CI pipeline
python comparemssql_bq.py \
  --bq-project-id ${GCP_PROJECT} \
  --bq-dataset ${BQ_DATASET} \
  --output-file validation_report.txt

# Fail the pipeline if validation fails
if [ $? -ne 0 ]; then
  echo "Data validation failed. Check validation_report.txt for details."
  exit 1
fi
```

## Troubleshooting

### Connection Issues
- Verify network connectivity to both BigQuery and MSSQL
- Check credentials and permissions
- Ensure firewall rules allow connections
- Make sure the connection string includes all required parameters and is properly formatted
- Test your connection string with a simple Python script using pyodbc.connect() before using it with this tool

### Validation Failures
- Check if ETL process completed successfully before running validation
- Verify table names and dataset names match exactly
- For schema validation issues, check data type mappings between MSSQL and BigQuery
- Ensure the database specified in your connection string contains the tables you're validating