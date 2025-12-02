"""
Main execution module for CSV to BigQuery ETL process
"""

import argparse
import concurrent.futures
import json
import logging
import os
import sys
from typing import Any, Dict, List, Optional


def process_single_file(
    bq_client: BigQueryClient, config: Dict[str, Any], csv_file: str, service: str
) -> Dict[str, Any]:
    """
    Process a single CSV file

    Args:
        bq_client: BigQuery client instance
        config: Configuration dictionary
        csv_file: CSV file path
        service: Service name

    Returns:
        Dictionary with processing result
    """
    try:
        # Extract table name from file path
        table_name = os.path.splitext(os.path.basename(csv_file))[0]

        # Construct GCS URI for this file
        gcs_uri = f"gs://{config.get('gcs_bucket')}/{csv_file}"

        # Get dataset name for this service using config template
        dataset_name = get_dataset_name(config, service)
        table_exists = bq_client.table_exists(dataset_name, table_name)

        # Create or update table
        if table_exists:
            success = bq_client.upsert_table_from_csv(
                dataset_name=dataset_name,
                table_name=table_name,
                gcs_uri=gcs_uri,
            )
            write_operation = "upsert"
        else:
            success = bq_client.create_table_from_csv(
                dataset_name=dataset_name,
                table_name=table_name,
                gcs_uri=gcs_uri,
                write_disposition="WRITE_TRUNCATE",
            )
            write_operation = "create"

        return {
            "file_path": csv_file,
            "table_name": table_name,
            "operation": write_operation,
            "success": success,
        }
    except Exception as e:
        logger.error(f"Error processing file {csv_file}: {e}")
        return {
            "file_path": csv_file,
            "table_name": os.path.splitext(os.path.basename(csv_file))[0],
            "operation": "unknown",
            "success": False,
            "error": str(e),
        }


try:
    from bigquery_client import BigQueryClient
    from CSV_reader import CSVReader
    from validator import Validator
except ImportError:
    from src.bigquery_client import BigQueryClient
    from src.CSV_reader import CSVReader
    from src.validator import Validator

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("csv2bigquery.log"),
        logging.StreamHandler(sys.stdout),
    ],
)

logger = logging.getLogger(__name__)


def load_config(config_path: str) -> Dict[str, Any]:
    """
    Load configuration from JSON file

    Args:
        config_path: Path to configuration file

    Returns:
        Dictionary with configuration parameters
    """
    try:
        with open(config_path, "r") as f:
            config = json.load(f)
        return config
    except Exception as e:
        logger.error(f"Error loading configuration from {config_path}: {e}")
        return {}


def initialize_clients(config: Dict[str, Any]) -> tuple[BigQueryClient, CSVReader]:
    """
    Initialize BigQuery client and CSV reader based on configuration

    Args:
        config: Configuration dictionary

    Returns:
        Tuple of (BigQueryClient, CSVReader)
    """
    # Initialize BigQuery client
    bq_client = BigQueryClient(
        project_id=config.get("project_id"),
        location=config.get("region"),
        service_account_path=config.get("service_account_path"),
    )

    # Initialize CSV reader
    csv_reader = CSVReader(
        gcs_bucket=config.get("gcs_bucket"),
        service_account_path=config.get("service_account_path"),
    )

    return bq_client, csv_reader


def create_datasets(bq_client: BigQueryClient, datasets: List[str]) -> None:
    """
    Create BigQuery datasets if they don't exist

    Args:
        bq_client: BigQuery client instance
        datasets: List of dataset names to create
    """
    for dataset in datasets:
        logger.info(f"Creating dataset: {dataset}")
        bq_client.create_dataset(dataset, exists_ok=True)


def get_dataset_name(config: Dict[str, Any], service: str) -> str:
    """
    Generate dataset name for a specific service using template

    Args:
        config: Configuration dictionary
        service: Service name

    Returns:
        Dataset name string
    """
    template = config.get("dataset_name_template", "dev_{service}_service")
    return template.format(service=service)


def get_gcs_path(config: Dict[str, Any], service: str, date: str) -> str:
    """
    Generate GCS path for a specific service using template

    Args:
        config: Configuration dictionary
        service: Service name
        date: Date string

    Returns:
        GCS path string
    """
    template = config.get(
        "gcs_base_path_template", "sql-exports/{date}/csvextract/{service}"
    )
    return template.format(date=date, service=service)


def process_service(
    bq_client: BigQueryClient,
    csv_reader: CSVReader,
    config: Dict[str, Any],
    service: str,
    date_folder: str = "20251201",
    specific_table: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Process a specific service

    Args:
        bq_client: BigQuery client instance
        csv_reader: CSV reader instance
        config: Configuration dictionary
        service: Service name

        date_folder: Date folder for the export

    Returns:
        Dictionary with processing results
    """
    logger.info(f"Processing service: {service}")

    # Construct GCS path for this service using config template
    service_gcs_path = get_gcs_path(config, service, date_folder)

    # List CSV files for this service
    csv_files = csv_reader.list_csv_files_in_gcs(service_gcs_path)
    if not csv_files:
        logger.warning(f"No CSV files found for service: {service}")
        return {
            "service": service,
            "status": "skipped",
            "message": "No CSV files found",
            "files_processed": 0,
        }

    results = {
        "service": service,
        "status": "success",
        "message": "Processing completed",
        "files_processed": 0,
        "files_results": [],
    }

    # Filter for specific table if provided
    if specific_table:
        csv_files = [
            f
            for f in csv_files
            if os.path.splitext(os.path.basename(f))[0] == specific_table
        ]
        if not csv_files:
            logger.warning(
                f"No CSV file found for table: {specific_table} in service: {service}"
            )
            return {
                "service": service,
                "status": "skipped",
                "message": f"No CSV file found for table: {specific_table}",
                "files_processed": 0,
            }

    # Process each CSV file in parallel for improved performance
    # Determine number of workers based on file count (limit to 10 to avoid API rate limits)
    max_file_workers = min(len(csv_files), 10)

    if max_file_workers > 1:
        logger.info(
            f"Processing {len(csv_files)} files in parallel with {max_file_workers} workers"
        )

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=max_file_workers
        ) as executor:
            futures = []
            for csv_file in csv_files:
                future = executor.submit(
                    process_single_file, bq_client, config, csv_file, service
                )
                futures.append(future)

            # Collect results as they complete
            for future in concurrent.futures.as_completed(futures):
                try:
                    file_result = future.result()
                    results["files_results"].append(file_result)

                    if file_result["success"]:
                        results["files_processed"] += 1
                    else:
                        results["status"] = "warning"
                        results["message"] = "Some files failed to process"
                except Exception as e:
                    logger.error(f"Error in parallel file processing: {e}")
                    results["files_results"].append(
                        {
                            "file_path": csv_file,
                            "table_name": os.path.splitext(os.path.basename(csv_file))[
                                0
                            ],
                            "operation": "unknown",
                            "success": False,
                            "error": str(e),
                        }
                    )
                    results["status"] = "warning"
                    results["message"] = "Some files failed to process"
    else:
        # Fall back to sequential processing for single file
        for csv_file in csv_files:
            try:
                # Fall back to sequential processing for single file
                for csv_file in csv_files:
                    try:
                        # Extract table name from file path
                        table_name = os.path.splitext(os.path.basename(csv_file))[0]

                        # Construct GCS URI for this file
                        gcs_uri = f"gs://{config.get('gcs_bucket')}/{csv_file}"

                        logger.info(
                            f"Processing file: {csv_file} -> table: {table_name}"
                        )

                        # Get dataset name for this service using config template
                        dataset_name = get_dataset_name(config, service)
                        table_exists = bq_client.table_exists(dataset_name, table_name)

                        # Create or update table
                        if table_exists:
                            success = bq_client.upsert_table_from_csv(
                                dataset_name=dataset_name,
                                table_name=table_name,
                                gcs_uri=gcs_uri,
                            )
                            write_operation = "upsert"
                        else:
                            success = bq_client.create_table_from_csv(
                                dataset_name=dataset_name,
                                table_name=table_name,
                                gcs_uri=gcs_uri,
                                write_disposition="WRITE_TRUNCATE",
                            )
                            write_operation = "create"

                        file_result = {
                            "file_path": csv_file,
                            "table_name": table_name,
                            "operation": write_operation,
                            "success": success,
                        }

                        results["files_results"].append(file_result)

                        if success:
                            results["files_processed"] += 1
                        else:
                            results["status"] = "warning"
                            results["message"] = "Some files failed to process"

                    except Exception as e:
                        logger.error(f"Error processing file {csv_file}: {e}")
                        results["files_results"].append(
                            {
                                "file_path": csv_file,
                                "table_name": os.path.splitext(
                                    os.path.basename(csv_file)
                                )[0],
                                "operation": "unknown",
                                "success": False,
                                "error": str(e),
                            }
                        )
                        results["status"] = "warning"
                        results["message"] = "Some files failed to process"
            except Exception as ex:
                logger.error(f"Error processing files: {ex}")
                results["status"] = "error"
                results["message"] = "Failed to process files"

    return results


def validate_single_service_table(
    validator: Validator,
    config: Dict[str, Any],
    service: str,
    table_name: str,
    date_folder: str = "20251201",
) -> Dict[str, Any]:
    """
    Validate a specific table in a specific service

    Args:
        validator: Validator instance
        config: Configuration dictionary
        service: Service name
        table_name: Table name to validate
        date_folder: Date folder for the export

    Returns:
        Dictionary with validation results
    """
    logger.info(f"Starting validation for service: {service}, table: {table_name}")

    dataset_name = get_dataset_name(config, service)
    service_gcs_path = get_gcs_path(config, service, date_folder)

    # Find the CSV file for this specific table
    csv_files = validator.csv_reader.list_csv_files_in_gcs(service_gcs_path)
    target_csv = None
    for csv_file in csv_files:
        csv_table_name = os.path.splitext(os.path.basename(csv_file))[0]
        if csv_table_name == table_name:
            target_csv = csv_file
            break

    if not target_csv:
        return {
            "status": "failed",
            "message": f"No CSV file found for table: {table_name}",
            "service": service,
            "table": table_name,
        }

    validation_results = {
        "status": "success",
        "message": "Validation completed",
        "service": service,
        "table": table_name,
    }

    # Run completeness validation
    completeness = validator.validate_single_file_completeness(
        dataset_name, target_csv, table_name
    )

    # Run correctness validation
    correctness = validator.validate_single_file_correctness(
        dataset_name, target_csv, table_name
    )

    validation_results["completeness"] = completeness
    validation_results["correctness"] = correctness

    if (
        completeness.get("status") != "success"
        or correctness.get("status") != "success"
    ):
        validation_results["status"] = "warning"
        validation_results["message"] = "Validation issues found"

    return validation_results


def validate_results(
    validator: Validator,
    config: Dict[str, Any],
    services: List[str],
    date_folder: str = "20251201",
) -> Dict[str, Any]:
    """
    Validate results for all services

    Args:
        validator: Validator instance
        config: Configuration dictionary
        services: List of services to validate

        date_folder: Date folder for the export

    Returns:
        Dictionary with validation results
    """
    logger.info("Starting validation of results")

    validation_results = {
        "status": "success",
        "message": "Validation completed",
        "services": {},
    }

    all_services_valid = True

    for service in services:
        dataset_name = get_dataset_name(config, service)
        service_gcs_path = get_gcs_path(config, service, date_folder)

        # Run completeness validation
        completeness = validator.validate_completeness_gcs(
            dataset_name, service_gcs_path
        )

        # Run correctness validation
        correctness = validator.validate_correctness_gcs(dataset_name, service_gcs_path)

        validation_results["services"][service] = {
            "completeness": completeness,
            "correctness": correctness,
            "status": "success"
            if completeness.get("status") == "success"
            and correctness.get("status") == "success"
            else "warning",
        }

        if (
            completeness.get("status") != "success"
            or correctness.get("status") != "success"
        ):
            all_services_valid = False

    if not all_services_valid:
        validation_results["status"] = "warning"
        validation_results["message"] = "Some services have validation issues"

    return validation_results


def main():
    """Main execution function"""
    parser = argparse.ArgumentParser(description="CSV to BigQuery ETL process")
    parser.add_argument(
        "--config",
        default="config.json",
        help="Path to configuration file (default: config.json)",
    )
    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="Only run validation, skip data processing",
    )
    parser.add_argument(
        "--service",
        help="Process only the specified service (if not provided, all services are processed)",
    )
    parser.add_argument(
        "--table",
        help="Process only the specified table in the specified service (requires --service)",
    )
    parser.add_argument(
        "--date",
        default="20251201",
        help="Date folder for the export (default: 20251201)",
    )
    parser.add_argument(
        "--rerun",
        action="store_true",
        help="Rerun processing for a specific service or table (requires --service, optionally --table)",
    )

    args = parser.parse_args()

    # Load configuration
    config = load_config(args.config)
    if not config:
        logger.error("Failed to load configuration. Exiting.")
        return 1

    # Initialize clients
    bq_client, csv_reader = initialize_clients(config)

    # Initialize validator
    validator = Validator(bq_client, csv_reader)

    # Get services list from configuration
    services = config.get(
        "services",
        [
            "auth-service",
            "career-service",
            "data-protection-service",
            "digital-credential-service",
            "document-service",
            "learning-service",
            "notification-service",
            "portfolio-service",
            "question-bank-service",
        ],
    )

    # Override with specific service if provided
    if args.service:
        if args.service not in services:
            logger.error(f"Invalid service: {args.service}")
            return 1
        services = [args.service]

    # Validate arguments
    if args.table and not args.service:
        logger.error("--table requires --service to be specified")
        return 1

    if args.rerun and not args.service:
        logger.error("--rerun requires --service to be specified")
        return 1

    # Create datasets for all services using config template
    datasets = [get_dataset_name(config, service) for service in services]
    create_datasets(bq_client, datasets)

    # Process services in parallel for improved performance
    processing_results = {}

    if not args.validate_only:
        logger.info("Starting data processing")

        # Determine number of workers based on services count (limit to 5 to avoid API rate limits)
        max_workers = min(len(services), 5)

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = []
            for service in services:
                specific_table = args.table if args.rerun else None
                future = executor.submit(
                    process_service,
                    bq_client,
                    csv_reader,
                    config,
                    service,
                    args.date,
                    specific_table,
                )
                futures.append(future)

            # Collect results as they complete
            for future in concurrent.futures.as_completed(futures):
                result = future.result()
                processing_results[result["service"]] = result

    # Validate results
    if args.rerun and args.table:
        # Validate only the specific table
        validation_results = validate_single_service_table(
            validator, config, args.service, args.table, args.date
        )
    else:
        # Validate all services
        validation_results = validate_results(validator, config, services, args.date)

    # Generate report
    report = {
        "config": args.config,
        "date": args.date,
        "validate_only": args.validate_only,
        "services_processed": services,
        "rerun": args.rerun,
        "specific_table": args.table if args.rerun else None,
        "processing_results": processing_results,
        "validation_results": validation_results,
    }

    # Save report to file
    report_file = f"csv2bq_report_{args.date}.json"
    with open(report_file, "w") as f:
        json.dump(report, f, indent=2)

    logger.info(f"Report saved to {report_file}")

    # Log summary
    logger.info("=== Processing Summary ===")
    for service, result in processing_results.items():
        logger.info(
            f"Service: {service}, Status: {result.get('status')}, Files: {result.get('files_processed')}"
        )

    logger.info("=== Validation Summary ===")
    if args.rerun and args.table:
        logger.info(
            f"Service: {args.service}, Table: {args.table}, Status: {validation_results.get('status')}"
        )
    else:
        for service, result in validation_results.get("services", {}).items():
            logger.info(f"Service: {service}, Status: {result.get('status')}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
