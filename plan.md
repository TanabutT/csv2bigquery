# Plan for CSV to BigQuery ETL Process

## Overview
This plan outlines the development and implementation of a Python-based ETL (Extract, Transform, Load) solution for processing CSV files and loading them into Google BigQuery as tables within specified datasets. The project is currently fully implemented with all major features operational.

## Project Status: ✅ COMPLETE

All major components have been implemented and are functional:
- ✅ BigQuery client with connection and table management
- ✅ CSV reader supporting both local and GCS sources
- ✅ Data validator with completeness and correctness checks
- ✅ Dynamic configuration system
- ✅ Rerun functionality for specific services and tables
- ✅ Comprehensive error handling and logging
- ✅ Test suite implementation
- ✅ Performance optimizations:
  - Parallel service processing (up to 5 services concurrently)
  - Parallel file processing within each service (up to 10 files concurrently)
  - Optimized code structure for faster execution

## Current Project Structure
```
csv2bigquery/
├── src/
│   ├── __init__.py              # Package initialization
│   ├── main.py                 # Main execution module with CLI
│   ├── bigquery_client.py       # BigQuery operations
│   ├── CSV_reader.py            # CSV file processing
│   └── validator.py             # Data validation
├── tests/
│   ├── __init__.py              # Test package initialization
│   ├── test_bigquery_client.py   # Tests for BigQuery client
│   ├── test_CSV_reader.py        # Tests for CSV reader
│   └── test_validator.py         # Tests for validator
├── credentials/                  # Directory for service account keys
├── .gitignore                  # Git ignore file
├── requirements.txt              # Python dependencies
├── config.json                 # Dynamic configuration file
├── README.md                   # Project documentation
└── plan.md                     # This file
```

## Configuration System

The configuration system uses templates for dynamic resource generation:

```json
{
  "project_id": "poc-piloturl-nonprod",
  "dataset_name_template": "dev_{service}_service",
  "region": "asia-southeast1",
  "gcs_bucket": "terra-mhesi-dp-poc-gcs-bronze-01",
  "gcs_base_path_template": "sql-exports/{date}/csvextract/{service}",
  "services": [
    "auth-service",
    "career-service",
    "data-protection-service",
    "digital-credential-service",
    "document-service",
    "learning-service",
    "notification-service",
    "portfolio-service",
    "question-bank-service"
  ],
  "service_account_path": "./credentials/service-account.json"
}
```

Key configuration features:
- `dataset_name_template`: Generates dataset names dynamically (e.g., "dev_career_service")
- `gcs_base_path_template`: Generates GCS paths dynamically (e.g., "sql-exports/20251201/csvextract/career-service")
- `services`: List of services to process

## Implemented Features

### 1. BigQuery Operations
- ✅ Authentication using service account or default credentials
- ✅ Dynamic dataset creation based on service name
- ✅ Table existence checking
- ✅ Table creation from CSV files with auto-detected schema
- ✅ Upsert operations (update existing or insert new records)
- ✅ Schema and metadata retrieval

### 2. CSV Processing
- ✅ Support for both local and GCS file sources
- ✅ CSV file discovery and metadata extraction
- ✅ Schema extraction from CSV files
- ✅ Data loading into pandas DataFrames

### 3. Data Validation
- ✅ Completeness validation:
  - Row count comparison between CSV and BigQuery
  - File processing verification
  - Detailed reporting of any discrepancies
- ✅ Correctness validation:
  - Schema validation between CSV and BigQuery
  - Sample data comparison
  - Data type consistency checking

### 4. Advanced Features
- ✅ Rerun functionality:
  - Rerun specific services: `--service career-service --rerun`
  - Rerun specific tables: `--service career-service --table users --rerun`
  - Validate only mode: `--validate-only`
- ✅ Dynamic service support via configuration
- ✅ Comprehensive error handling and logging
- ✅ Detailed reporting in JSON format

## Command-Line Interface

### Basic Usage
```bash
# Process all services
python src/main.py

# Process specific service
python src/main.py --service career-service

# Process files from specific date
python src/main.py --date 20251202

# Run validation only
python src/main.py --validate-only
```

### Advanced Usage
```bash
# Rerun a specific service
python src/main.py --service career-service --rerun

# Rerun a specific table within a service
python src/main.py --service career-service --table users --rerun

# Validate only a specific table
python src/main.py --service career-service --table users --validate-only --rerun
```

## Test Suite

The project includes a comprehensive test suite:
- `test_bigquery_client.py`: Tests for BigQuery operations
- `test_CSV_reader.py`: Tests for CSV processing
- `test_validator.py`: Tests for validation logic

Run tests with:
```bash
python -m unittest discover tests
```

## Deployment and Replication

To replicate this project in a new environment:

1. **Clone the Repository**:
   ```bash
   git clone https://github.com/TanabutT/csv2bigquery.git
   cd csv2bigquery
   ```

2. **Set Up Python Environment**:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   pip install -r requirements.txt
   ```

3. **Configure the Application**:
   - Copy `config.json.example` to `config.json` (if available)
   - Update configuration parameters as needed:
     - `project_id`: Your GCP project ID
     - `gcs_bucket`: Your GCS bucket name
     - `service_account_path`: Path to service account key

4. **Place Service Account Key**:
   - Put your service account JSON key in `credentials/` directory

5. **Run the Application**:
   ```bash
   # Process all services
   python src/main.py
   
   # Or process specific service
   python src/main.py --service career-service
   ```

## Architecture Flow

1. **Initialization**:
   - Load configuration from `config.json`
   - Initialize BigQuery client and CSV reader
   - Initialize validator

2. **Processing**:
   - For each service (or specific service):
     - Generate dataset name using template
     - Generate GCS path using template
     - Create dataset if not exists
     - List CSV files in GCS location
     - For each CSV file:
       - Extract table name from filename
       - Check if table exists in BigQuery
       - Create new table or upsert data
       - Track processing results

3. **Validation**:
   - For each processed service:
     - Run completeness validation
     - Run correctness validation
     - Generate validation report

4. **Reporting**:
   - Generate comprehensive JSON report
   - Log summary to console and file

## Error Handling

The application implements comprehensive error handling:
- Connection errors (GCS, BigQuery)
- File access errors
- Data processing errors
- Validation failures
- Partial failures with continued processing

All errors are logged with appropriate detail levels, and processing continues where possible.

## Performance Optimizations Implemented

### 1. Parallel Service Processing
The system now processes multiple services concurrently using Python's ThreadPoolExecutor:
- Processes up to 5 services simultaneously (configurable)
- Each service's CSV discovery and processing happens independently
- Significantly reduces total execution time for multiple services

### 2. Parallel File Processing
Within each service, multiple CSV files are processed in parallel:
- Processes up to 10 files simultaneously (configurable)
- File loading operations to BigQuery run concurrently
- Optimizes throughput for services with many CSV files

### 3. Resource Management
The implementation includes intelligent resource management:
- Thread pool limits prevent API rate limit issues
- Automatic fallback to sequential processing when needed
- Proper error handling that doesn't affect other parallel tasks

### 4. Performance Metrics
Expected performance improvements:
- **3-5x speed improvement** for processing multiple services
- **2-10x speed improvement** for services with multiple files
- **Overall 10-15x improvement** for complete ETL process

## Future Enhancements

Potential areas for future improvement:
1. **Dependency Management**:
   - Track table dependencies
   - Implement proper execution order
   - Add dependency visualization

2. **Additional Performance Optimization**:
   - Batch operations for large datasets
   - Incremental loading for large files
   - Connection pooling for BigQuery client

3. **Advanced Validation**:
   - Data quality checks
   - Anomaly detection
   - Custom validation rules

4. **Monitoring**:
   - Progress tracking
   - Performance metrics
   - Alerting for failures

## Examples

### Processing a Single Service
```bash
python src/main.py --service career-service --date 20251201
```

Result:
- Creates dataset: `dev_career_service`
- Processes CSV files from: `sql-exports/20251201/csvextract/career-service`
- Generates table for each CSV file
- **Processes files in parallel for faster execution**
- Validates all processed data
- Creates report: `csv2bq_report_20251201.json`

### Rerunning a Specific Table
```bash
python src/main.py --service career-service --table users --rerun
```

Result:
- Processes only `users.csv` from career-service
- Upserts data into existing `dev_career_service.users` table
- Validates only this table
- Creates report with details for this specific operation

This plan serves as both historical documentation and a practical guide for replicating the project in new environments.

## Performance Tips

For optimal performance when running this ETL process:

1. **Adjust Worker Limits**:
   - Increase `max_workers` in the ThreadPoolExecutor if you have a powerful machine
   - Decrease if you encounter API rate limit errors
   - Monitor resource usage to find optimal settings

2. **Monitor API Usage**:
   - Track BigQuery API quotas during execution
   - Consider staggering large processing jobs
   - Use separate GCS buckets for different environments

3. **Optimize CSV Files**:
   - Ensure CSV files have proper headers
   - Use consistent data types across files
   - Remove unnecessary columns before processing

4. **Batch Processing**:
   - Group similar services together when possible
   - Process services with fewer files first
   - Use appropriate date ranges to limit data volume
