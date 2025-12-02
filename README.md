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
- `dataset_name`: Name of the BigQuery dataset (note: this is used as a base name, and multiple datasets will be created for each service)
- `region`: Location/region for the BigQuery datasets
- `gcs_bucket`: Name of the GCS bucket containing CSV files
- `gcs_base_path`: Base path in GCS where CSV files are stored
- `service_account_path`: Path to service account key file (optional)

## Supported Services

The application supports the following services:
- dev-auth-service
- dev-career-service
- dev-data-protection-service
- dev-digital-credential-service
- dev-document-service
- dev-learning-service
- dev-notification-service
- dev-portfolio-service
- dev-question-bank-service

For each service, a corresponding dataset will be created in BigQuery (e.g., "dev_auth_service" for "auth-service").

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

2. **Correctness Validation**
   - Validates schema consistency between CSV files and BigQuery tables
   - Samples and compares data values between source and destination

## Error Handling

The application includes comprehensive error handling:
- GCS connection errors
- BigQuery operation failures
- CSV parsing errors
- Validation failures

Errors are logged with appropriate detail levels, and the application continues processing other files when possible.

## Testing

Run the test suite:
```bash
python -m unittest discover tests
```

## Development

To extend the application:

1. Add new services to the services list in `main.py`
2. Modify the configuration schema in `config.json` as needed
3. Add new validation rules in `validator.py`
4. Extend the BigQuery operations in `bigquery_client.py`

## License

This project is licensed under the MIT License.
