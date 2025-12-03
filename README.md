# CSV to BigQuery ETL Process

This project provides a Python-based ETL (Extract, Transform, Load) solution for processing CSV files and loading them into Google BigQuery as tables within specified datasets.

## Overview

The application automates the process of extracting data from CSV files stored in Google Cloud Storage (or local filesystem), transforming it as needed, and loading it into BigQuery tables. It includes comprehensive validation to ensure data integrity between source and destination.

## Features

- Supports CSV files from Google Cloud Storage or local filesystem
- Automatically creates BigQuery datasets and tables if they don't exist
- Handles upsert operations (update existing rows or insert new ones)
- Comprehensive validation of both completeness and correctness of data
- Generates detailed reports of processing and validation results
- Configurable service account authentication
- Supports multiple services/datasets in a single run
- **NEW:** Rerun specific services or tables with the --rerun flag
- **NEW:** Target specific tables within a service for focused processing or validation
- **NEW:** Automatic filtering of prisma migration files
- **NEW:** Fixed CSV file discovery in GCS paths with dev- prefix

## Project Structure

```
csv2bigquery/
├── src/
│   ├── main.py                 # Main execution module
│   ├── bigquery_client.py      # BigQuery connection and operations
│   ├── CSV_reader.py          # CSV file processing
│   └── validator.py           # Data validation
├── tests/
│   ├── test_bigquery_client.py # Tests for BigQuery client
│   ├── test_CSV_reader.py     # Tests for CSV reader
│   └── test_validator.py      # Tests for validator
├── requirements.txt           # Python dependencies
├── config.json               # Configuration file
└── README.md                 # This file
```

## Setup and Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/TanabutT/csv2bigquery.git
   cd csv2bigquery
   ```

2. Create a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows use venv\Scripts\activate
   ```

3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

4. Configure the application by editing `config.json`:
   ```json
   {
     "project_id": "your-gcp-project-id",
     "dataset_name": "dev_career_service",
     "region": "asia-southeast1",
     "gcs_bucket": "your-gcs-bucket-name",
     "gcs_base_path": "sql-exports/20251201/csvextract/dev-career-service",
     "service_account_path": "./credentials/service-account.json"
   }
   ```

5. Place your service account JSON key file in the `credentials` directory.

## Configuration

The application uses the following configuration parameters:

- `project_id`: Your Google Cloud project ID
- `dataset_name_template`: Template for generating dataset names (e.g., "dev_{service}")
- `region`: Location/region for the BigQuery datasets
- `gcs_bucket`: Name of the GCS bucket containing CSV files
- `gcs_base_path_template`: Template for generating GCS paths dynamically (e.g., "sql-exports/{date}/csvextract/{service}")
- `services`: List of services to process
- `service_account_path`: Path to the service account key file (optional)

MSSQL validation (optional)
If you'd like to validate BigQuery tables directly against a SQL Server source instead of a CSV source, add an `mssql` section to your `config.json` (optional):

```json
"mssql": {
   "server": "sql.example.com",
   "database": "db_name",
   "username": "sql_user",
   "password": "secret",
   "driver": "{ODBC Driver 17 for SQL Server}",
   "timeout": 30
}
```

You can select the validation source on the command line using `--validate-source` (default `gcs`). Supported values: `gcs` (validate against CSV files in GCS) or `mssql` (validate directly against SQL Server):

```bash
# Validate using MSSQL as the source
python src/main.py --validate-only --validate-source mssql
```

Additional note about region enforcement
- `enforce_dataset_location` (optional, default True): When performing upserts, the client will check the dataset's actual region and by default will abort the upsert if it doesn't match the configured `region` in `config.json`. This prevents accidental cross-region temporary table creation and Merge queries failing due to region mismatches. Set `enforce_dataset_location` to False if you want to allow operations to proceed despite a dataset/region mismatch (not recommended).

## Supported Services

The application supports the following services (configured in config.json):
- auth-service
- career-service
- data-protection-service
- digital-credential-service
- document-service
- learning-service
- notification-service
- portfolio-service
- question-bank-service

For each service, a corresponding dataset will be created in BigQuery (e.g., "dev_auth" for "auth-service").

Note: The actual GCS directories include a "dev-" prefix (e.g., dev-auth-service), which the application handles automatically.

## Usage

### Basic Usage

Process all services:
```bash
python src/main.py
```

Process only a specific service:
```bash
python src/main.py --service career-service
```

Process files from a specific date:
```bash
python src/main.py --date 20251202
```

Run only validation (skip data processing):
```bash
python src/main.py --validate-only
```

Use a custom configuration file:
```bash
python src/main.py --config /path/to/config.json
```

### Advanced Usage (New Features)

Rerun a specific service:
```bash
python src/main.py --service career-service --rerun
```

Rerun a specific table within a service:
```bash
python src/main.py --service career-service --table users --rerun
```

Validate only a specific table:
```bash
python src/main.py --service career-service --table users --validate-only --rerun
```

### Command Line Options

- `--config`: Path to configuration file (default: config.json)
- `--validate-only`: Only run validation, skip data processing
- `--service`: Process only the specified service
- `--table`: Process only the specified table in the specified service
- `--date`: Date folder for the export (default: 20251201)
- `--rerun`: Rerun processing for a specific service or table

## Output

The application generates:

1. A log file (`csv2bigquery.log`) with detailed processing information
2. A JSON report file (`csv2bq_report_DATE.json`) containing:
   - Processing results for each service and file
   - Validation results for completeness and correctness
   - Summary statistics

## Validation

The application performs two types of validation:

1. **Completeness Validation**
   - Verifies all CSV files in the source have been processed
   - Compares row counts between source CSV files and destination tables
   - Automatically excludes prisma migration files from validation

2. **Correctness Validation**
   - Validates schema consistency between CSV files and BigQuery tables
   - Samples and compares data values between source and destination
   - Reports data type mismatches and inconsistencies

## Error Handling

The application includes comprehensive error handling:
- GCS connection errors
- BigQuery operation failures
- CSV parsing errors
- Validation failures
- Dataset naming issues (automatically handles hyphen to underscore conversion)

Errors are logged with appropriate detail levels, and the application continues processing other files when possible.

## Recent Fixes and Improvements

1. **CSV Discovery Fix**: Fixed GCS path resolution to correctly find CSV files in directories with "dev-" prefix
2. **Dataset Naming**: Corrected dataset name generation to handle hyphens in service names properly
3. **Prisma File Filtering**: Added automatic filtering to skip prisma migration files during processing
4. **CSV Reader Enhancement**: Improved CSV reading to use GCS client directly instead of public URLs
5. **Region/Location Fix**: Ensure BigQuery load jobs use the configured `region`/location so temporary tables are created in the same dataset region to avoid cross-region errors (e.g., Not found: Table ..._temp was not found in location). The `BigQueryClient` now uses the configured region for load jobs.

## Testing

Run the test suite:
```bash
python -m unittest discover tests
```

## Development

To extend the application:

1. Add new services to the services list in `config.json`
2. Modify the configuration schema in `config.json` as needed
3. Add new validation rules in `validator.py`
4. Extend the BigQuery operations in `bigquery_client.py`

## Troubleshooting

### CSV Files Not Found

If you encounter errors like "Found 0 CSV files in GCS path: sql-exports/20251201/csvextract/question-bank-service":

1. **Check GCS Directory Structure**: 
   - CSV files are stored in directories with `dev-` prefix (e.g., `dev-question-bank-service`)
   - The application automatically adds this prefix when searching for files

2. **Verify Configuration**:
   - Ensure `gcs_base_path_template` in config.json is correct
   - The template should be: `sql-exports/{date}/csvextract/{service}`

3. **Check Service Names**:
   - Service names in config.json should use kebab-case (e.g., `question-bank-service`)
   - Dataset names will automatically have hyphens converted to underscores for BigQuery compatibility

4. **Prisma Files**:
   - Prisma migration files are automatically filtered out
   - They contain database schema changes, not actual data

## License

This project is licensed under the MIT License.
