# Plan for CSV to BigQuery ETL Process

## Overview
This plan outlines the development of a Python script to process CSV files and load them into Google BigQuery as tables within a specified dataset.

## Project Structure
CSV2bigquery/
├── src/
│   ├── main.py
│   ├── bigquery_client.py
│   ├── CSV_reader.py
│   └── validator.py
├── tests/
│   ├── test_bigquery_client.py
│   ├── test_CSV_reader.py
│   └── test_validator.py
├── requirements.txt
├── config.json
└── README.md


## Implementation Steps
### 1. git init and git ignore
- Initialize a new Git repository 
- Create `.gitignore` file to exclude unnecessary files
### 2. Setup and Dependencies
- Create `requirements.txt` with necessary packages:
  - `google-cloud-bigquery`
  - `pandas`
  - `pyarrow`
  - `google-auth`
- create venv and install dependencies
  ```
  python -m venv venv
  source venv/bin/activate
  pip install -r requirements.txt
  ```
### 3. Configuration Management
- Create a `config.json` file for project settings:
  - GCP project ID
  - BigQuery dataset name (`dev-career-service`)
  - Location/region for dataset
  - Source CSV file path
  - Service account key path (if needed)

### 3. BigQuery Connection Module (`bigquery_client.py`)
- Initialize BigQuery client with proper authentication
- Function to create dataset if it doesn't exist
- Function to check if table exists
- Function to create table from CSV file
- Function to get table schema and metadata

### 4. CSV Reader Module (`CSV_reader.py`)
- Function to scan directory for CSV files in google cloud storage
- Function to read CSV file metadata
- Function to extract schema from CSV files
- Function to read CSV data into pandas DataFrame for validation

### 5. Validation Module (`validator.py`)
- Function to validate completeness:
  - Compare row counts between CSV and BigQuery table
  - Check if all files in source folder have been processed
- Function to validate correctness:
  - Sample data comparison between source and destination
  - Schema validation between CSV and BigQuery table
  - Data type consistency check

### 6. Main Execution Module (`main.py`)
- Initialize components
- Orchestrate the ETL process:
  1. Connect to BigQuery
  2. Ensure dataset exists
  3. Scan for CSV files
  4. Process each CSV file:
     - Create table in BigQuery if not exists
     - Load data from CSV to table
  5. Run validation checks
  6. Report results

## Detailed Implementation Plan

### Step 1: BigQuery Service Connection
- Implement authentication using service account or default credentials
- Initialize BigQuery client
- Test connection with a simple query or operation

### Step 2: CSV File Discovery
- Implement directory scanning to find all CSV files
- Extract file metadata (size, modified date)
- Create a list of files to be processed

### Step 3: Dataset Creation
- Check if dataset `dev-career-service` exists
- Create dataset if it doesn't exist with proper configuration
- Handle errors and exceptions

### Step 4: Table Creation from CSV
- For each CSV file:
  - Extract schema from CSV file
    - "dataset_name": "dev_career_service" in config.json file actually have 9 services :
          -dev-auth-service
          -dev-career-service
          -dev-data-protection-service
          -dev-digital-credential-service
          -dev-document-service
          -dev-learning-service
          -dev-notification-service
          -dev-portfolio-service
          -dev-question-bank-service
    - "gcs_base_path": "sql-exports-parquet/20251201/parquetextract/dev-career-service" this must access to the correct path as per the list of services above    
  - Determine appropriate table name (based on filename)
  - Create table in BigQuery with auto-detected schema
  - Load data from CSV file to table
  - Handle table updates vs. new tables (upsert operation)
    - If table exists, perform an upsert operation
    - If table doesn't exist, create a new table and load data

### Step 5: Validation Implementation
- **Completeness Validation**:
  - Verify row count matches between source CSV and destination table
  - Check that all CSV files in folder have been processed
  - Log any missing files or data discrepancies
  
- **Correctness Validation**:
  - Sample rows from both source and destination
  - Compare values between CSV and BigQuery table
  - Validate that schema types match between CSV and BigQuery
  - Check for data truncation or precision loss
  - Generate a validation report

### Step 6: Error Handling and Logging
- Implement comprehensive error handling
- Create logging mechanism for process tracking
- Handle partial failures gracefully



## Example Implementation Snippets

### Configuration Example
```json
{
  "project_id": "poc-piloturl-nonprod",
  "dataset_name": "dev_career_service",
  "region": "asia-southeast1",
  "parquet_source_path_folder": "/Users/tanabut.t/Documents/PlanB_project/gcp_cloudcomposer/parquet2bigquery/source_data",
  "parquet_source_path": "/Users/tanabut.t/Documents/PlanB_project/gcp_cloudcomposer/parquet2bigquery/source_data/*.csv",
  "gcs_bucket": "terra-mhesi-dp-poc-gcs-bronze-01",
  "gcs_base_path": "sql-exports-parquet/20251201/parquetextract/dev-career-service",
  "service_account_path": "./credentials/service-account.json"
}
in the future the CSV_source_path will move to google cloud storage gs://terra-mhesi-dp-poc-gcs-bronze-01/
