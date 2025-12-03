"""
Comparison script for validating data integrity between MSSQL source and BigQuery destination
"""

import argparse
import logging
import os
import sys
from typing import List, Optional

try:
    from bigquery_client import BigQueryClient
    from mssql_client import MSSQLClient
    from validator_mssql import Validator
except ImportError:
    from src.bigquery_client import BigQueryClient
    from src.mssql_client import MSSQLClient
    from src.validator_mssql import Validator

# Load environment variables from .env file
try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass  # dotenv not available, user must have environment variables set

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def main():
    """Main function to run validation between MSSQL and BigQuery"""
    parser = argparse.ArgumentParser(
        description="Validate data integrity between MSSQL and BigQuery"
    )
    parser.add_argument(
        "--bq-project-id", required=True, help="Google Cloud project ID for BigQuery"
    )
    parser.add_argument(
        "--bq-dataset", required=True, help="BigQuery dataset name to validate"
    )
    parser.add_argument(
        "--mssql-connection-string",
        help="MSSQL connection string (overrides SQL_CONNECTION_STRING env var)",
    )
    parser.add_argument(
        "--tables",
        nargs="*",
        help="Specific tables to validate (if not provided, will validate all tables)",
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=100,
        help="Number of rows to sample for validation (default: 100)",
    )
    parser.add_argument(
        "--output-file",
        help="Optional file path to save the validation report",
    )
    parser.add_argument(
        "--validation-type",
        choices=["completeness", "correctness", "both"],
        default="both",
        help="Type of validation to perform (default: both)",
    )

    args = parser.parse_args()

    try:
        # Initialize BigQuery client
        logger.info("Initializing BigQuery client...")
        bq_client = BigQueryClient(args.bq_project_id)

        # Initialize MSSQL client using connection string approach from testmssqlcon.py
        logger.info("Initializing MSSQL client...")

        # Get connection string from argument or environment variable
        connection_string = args.mssql_connection_string or os.getenv(
            "SQL_CONNECTION_STRING"
        )

        if not connection_string:
            logger.error(
                "No MSSQL connection string provided. Use --mssql-connection-string or set SQL_CONNECTION_STRING env var."
            )
            sys.exit(1)

        mssql_client = MSSQLClient(connection_string=connection_string)

        # Initialize validator
        validator = Validator(
            bigquery_client=bq_client,
            mssql_client=mssql_client,
            sample_size=args.sample_size,
        )

        # Run validations based on the selected type
        validation_results = {}
        reports = []

        if args.validation_type in ["completeness", "both"]:
            logger.info("Running completeness validation...")
            completeness_results = validator.validate_completeness_mssql(
                dataset_name=args.bq_dataset,
                tables=args.tables,
            )
            validation_results["completeness"] = completeness_results
            reports.append(
                "=== COMPLETENESS VALIDATION REPORT ===\n"
                + validator.generate_validation_report(completeness_results)
                + "\n"
            )

        if args.validation_type in ["correctness", "both"]:
            logger.info("Running correctness validation...")
            correctness_results = validator.validate_correctness_mssql(
                dataset_name=args.bq_dataset,
                tables=args.tables,
            )
            validation_results["correctness"] = correctness_results
            reports.append(
                "=== CORRECTNESS VALIDATION REPORT ===\n"
                + validator.generate_validation_report(correctness_results)
                + "\n"
            )

        # Combine all reports
        full_report = "\n".join(reports)

        # Print the report to console
        print(full_report)

        # Save report to file if specified
        if args.output_file:
            with open(args.output_file, "w") as f:
                f.write(full_report)
            logger.info(f"Validation report saved to {args.output_file}")

        # Determine exit code based on validation results
        has_failures = False
        for validation_type, results in validation_results.items():
            status = results.get("status", "unknown")
            if status == "failed":
                has_failures = True
                logger.error(f"{validation_type.capitalize()} validation failed")
            elif status == "warning":
                logger.warning(
                    f"{validation_type.capitalize()} validation completed with warnings"
                )

        sys.exit(1 if has_failures else 0)

    except Exception as e:
        logger.error(f"Validation process failed: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
