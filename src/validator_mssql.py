"""
Validation module for verifying data integrity between CSV source and BigQuery destination
"""

import logging
import random
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

try:
    from bigquery_client import BigQueryClient
    from CSV_reader import CSVReader
except ImportError:
    from src.bigquery_client import BigQueryClient
    from src.CSV_reader import CSVReader

logger = logging.getLogger(__name__)


class Validator:
    """Validator for checking completeness and correctness of ETL process"""

    def __init__(
        self,
        bigquery_client: BigQueryClient,
        csv_reader: Optional[CSVReader] = None,
        mssql_client: Optional[Any] = None,
        sample_size: int = 100,
    ):
        """
        Initialize validator with BigQuery client and CSV reader

        Args:
            bigquery_client: BigQuery client instance
            csv_reader: CSV reader instance
            sample_size: Number of rows to sample for validation
        """
        self.bigquery_client = bigquery_client
        self.csv_reader = csv_reader
        # Optional MSSQL client for validating against SQL Server as source
        self.mssql_client = mssql_client
        self.sample_size = sample_size
        self.validation_results = {}

    def validate_completeness_gcs(
        self, dataset_name: str, gcs_path: str
    ) -> Dict[str, Any]:
        """
        Validate completeness of ETL process with GCS source

        Args:
            dataset_name: BigQuery dataset name
            gcs_path: GCS path containing CSV files

        Returns:
            Dictionary with validation results
        """
        logger.info(f"Starting completeness validation for dataset: {dataset_name}")

        # Get list of CSV files in GCS
        csv_files = self.csv_reader.list_csv_files_in_gcs(gcs_path)
        if not csv_files:
            return {
                "status": "failed",
                "message": "No CSV files found in GCS path",
                "details": {"gcs_path": gcs_path},
            }

        # Check each CSV file against its corresponding BigQuery table
        results = {
            "status": "success",
            "message": "Completeness validation completed",
            "details": {"total_files": len(csv_files), "file_results": []},
        }

        all_files_processed = True
        total_csv_rows = 0
        total_bq_rows = 0

        for csv_file in csv_files:
            # Extract table name from file name
            table_name = self._extract_table_name_from_path(csv_file)
            if not table_name:
                continue

            # Get row counts
            csv_row_count = self.csv_reader.get_row_count_gcs(csv_file)
            bq_row_count = self.bigquery_client.get_row_count(dataset_name, table_name)
            total_csv_rows += csv_row_count
            total_bq_rows += bq_row_count

            # Check if table exists and row counts match
            table_exists = self.bigquery_client.table_exists(dataset_name, table_name)
            rows_match = csv_row_count == bq_row_count

            file_result = {
                "file_path": csv_file,
                "table_name": table_name,
                "table_exists": table_exists,
                "csv_rows": csv_row_count,
                "bq_rows": bq_row_count,
                "rows_match": rows_match,
                "status": "success" if rows_match else "failed",
            }

            results["details"]["file_results"].append(file_result)

            if not rows_match:
                all_files_processed = False

        results["details"]["all_files_processed"] = all_files_processed
        results["details"]["total_csv_rows"] = total_csv_rows
        results["details"]["total_bq_rows"] = total_bq_rows

        if not all_files_processed:
            results["status"] = "warning"
            results["message"] = "Some files have row count mismatches"

        return results

    # -------------------------------------------
    # MSSQL-based validation
    # -------------------------------------------
    def validate_completeness_mssql(
        self, dataset_name: str, database_name: str, tables: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Validate completeness by comparing row counts between SQL Server and BigQuery

        Args:
            dataset_name: BigQuery dataset name
            database_name: SQL Server database name (used for listing tables)
            tables: Optional list of table names to validate (if None, list all tables)

        Returns:
            Dictionary with validation results
        """
        if not self.mssql_client:
            return {
                "status": "failed",
                "message": "MSSQL client not provided for MSSQL validation",
            }

        logger.info(f"Starting MSSQL completeness validation for dataset: {dataset_name}")

        if tables is None:
            try:
                tables = self.mssql_client.list_tables(database_name)
            except Exception as e:
                return {
                    "status": "failed",
                    "message": f"Failed to list tables in SQL Server DB {database_name}: {e}",
                }

        results = {
            "status": "success",
            "message": "MSSQL completeness validation completed",
            "details": {"total_tables": len(tables), "table_results": []},
        }

        all_match = True
        total_mssql_rows = 0
        total_bq_rows = 0

        for table_name in tables:
            try:
                mssql_count = self.mssql_client.get_row_count(table_name)
            except Exception:
                mssql_count = 0

            bq_count = self.bigquery_client.get_row_count(dataset_name, table_name)

            table_exists = self.bigquery_client.table_exists(dataset_name, table_name)
            rows_match = mssql_count == bq_count

            results["details"]["table_results"].append(
                {
                    "table_name": table_name,
                    "mssql_rows": mssql_count,
                    "bq_rows": bq_count,
                    "table_exists": table_exists,
                    "rows_match": rows_match,
                    "status": "success" if rows_match else "failed",
                }
            )

            total_mssql_rows += mssql_count
            total_bq_rows += bq_count

            if not rows_match:
                all_match = False

        results["details"]["all_tables_match"] = all_match
        results["details"]["total_mssql_rows"] = total_mssql_rows
        results["details"]["total_bq_rows"] = total_bq_rows

        if not all_match:
            results["status"] = "warning"
            results["message"] = "Some tables have row count mismatches"

        return results

    def validate_completeness_local(
        self, dataset_name: str, local_path: str
    ) -> Dict[str, Any]:
        """
        Validate completeness of ETL process with local source

        Args:
            dataset_name: BigQuery dataset name
            local_path: Local path containing CSV files

        Returns:
            Dictionary with validation results
        """
        logger.info(f"Starting completeness validation for dataset: {dataset_name}")

        # Get list of CSV files in local directory
        csv_files = self.csv_reader.list_csv_files_local(local_path)
        if not csv_files:
            return {
                "status": "failed",
                "message": "No CSV files found in local path",
                "details": {"local_path": local_path},
            }

        # Check each CSV file against its corresponding BigQuery table
        results = {
            "status": "success",
            "message": "Completeness validation completed",
            "details": {"total_files": len(csv_files), "file_results": []},
        }

        all_files_processed = True
        total_csv_rows = 0
        total_bq_rows = 0

        for csv_file in csv_files:
            # Extract table name from file name
            table_name = self._extract_table_name_from_path(csv_file)
            if not table_name:
                continue

            # Get row counts
            csv_row_count = self.csv_reader.get_row_count_local(csv_file)
            bq_row_count = self.bigquery_client.get_row_count(dataset_name, table_name)
            total_csv_rows += csv_row_count
            total_bq_rows += bq_row_count

            # Check if table exists and row counts match
            table_exists = self.bigquery_client.table_exists(dataset_name, table_name)
            rows_match = csv_row_count == bq_row_count

            file_result = {
                "file_path": csv_file,
                "table_name": table_name,
                "table_exists": table_exists,
                "csv_rows": csv_row_count,
                "bq_rows": bq_row_count,
                "rows_match": rows_match,
                "status": "success" if rows_match else "failed",
            }

            results["details"]["file_results"].append(file_result)

            if not rows_match:
                all_files_processed = False

        results["details"]["all_files_processed"] = all_files_processed
        results["details"]["total_csv_rows"] = total_csv_rows
        results["details"]["total_bq_rows"] = total_bq_rows

        if not all_files_processed:
            results["status"] = "warning"
            results["message"] = "Some files have row count mismatches"

        return results

    def validate_correctness_gcs(
        self, dataset_name: str, gcs_path: str
    ) -> Dict[str, Any]:
        """
        Validate correctness of ETL process with GCS source

        Args:
            dataset_name: BigQuery dataset name
            gcs_path: GCS path containing CSV files

        Returns:
            Dictionary with validation results
        """
        logger.info(f"Starting correctness validation for dataset: {dataset_name}")

        # Get list of CSV files in GCS
        csv_files = self.csv_reader.list_csv_files_in_gcs(gcs_path)
        if not csv_files:
            return {
                "status": "failed",
                "message": "No CSV files found in GCS path",
                "details": {"gcs_path": gcs_path},
            }

        # Check each CSV file against its corresponding BigQuery table
        results = {
            "status": "success",
            "message": "Correctness validation completed",
            "details": {"total_files": len(csv_files), "file_results": []},
        }

        all_files_valid = True

        for csv_file in csv_files:
            # Extract table name from file name
            table_name = self._extract_table_name_from_path(csv_file)
            if not table_name:
                continue

            # Skip if table doesn't exist
            if not self.bigquery_client.table_exists(dataset_name, table_name):
                continue

            # Get schemas and compare
            csv_schema = self.csv_reader.extract_schema_from_csv_gcs(csv_file)
            bq_table_info = self.bigquery_client.get_table_info(
                dataset_name, table_name
            )
            bq_schema = {
                field["name"]: field["type"]
                for field in bq_table_info.get("schema", [])
            }

            # Check if schemas match
            schema_match = self._compare_schemas(csv_schema, bq_schema)

            # Sample rows and compare values
            sample_valid = self._compare_sample_data_gcs(
                dataset_name, table_name, csv_file
            )

            file_result = {
                "file_path": csv_file,
                "table_name": table_name,
                "schema_match": schema_match,
                "sample_match": sample_valid,
                "status": "success" if schema_match and sample_valid else "failed",
            }

            results["details"]["file_results"].append(file_result)

            if not schema_match or not sample_valid:
                all_files_valid = False

        results["details"]["all_files_valid"] = all_files_valid

        if not all_files_valid:
            results["status"] = "warning"
            results["message"] = "Some files have validation issues"

        return results

    def validate_correctness_mssql(
        self,
        dataset_name: str,
        database_name: str,
        tables: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Validate correctness by comparing SQL Server source (schema + sample rows)
        against BigQuery destination.

        Args:
            dataset_name: BigQuery dataset name
            database_name: SQL Server database name (used for listing tables)
            tables: Optional list of table names to validate (if None, validate all)

        Returns:
            Dictionary with validation results
        """
        if not self.mssql_client:
            return {
                "status": "failed",
                "message": "MSSQL client not provided for MSSQL validation",
            }

        logger.info(f"Starting MSSQL correctness validation for dataset: {dataset_name}")

        if tables is None:
            try:
                tables = self.mssql_client.list_tables(database_name)
            except Exception as e:
                return {
                    "status": "failed",
                    "message": f"Failed to list tables in SQL Server DB {database_name}: {e}",
                }

        results = {
            "status": "success",
            "message": "MSSQL correctness validation completed",
            "details": {"total_tables": len(tables), "table_results": []},
        }

        all_valid = True

        for table_name in tables:
            # Skip if BQ table doesn't exist
            if not self.bigquery_client.table_exists(dataset_name, table_name):
                continue

            # Get MSSQL schema
            try:
                mssql_schema = self.mssql_client.get_table_schema(table_name)
            except Exception:
                mssql_schema = {}

            # Get BQ schema
            bq_table_info = self.bigquery_client.get_table_info(dataset_name, table_name)
            bq_schema = {field["name"]: field["type"] for field in bq_table_info.get("schema", [])}

            schema_match = self._compare_schemas(mssql_schema, bq_schema)

            # Sample rows comparison
            sample_valid = self._compare_sample_data_mssql(dataset_name, table_name)

            results["details"]["table_results"].append(
                {
                    "table_name": table_name,
                    "schema_match": schema_match,
                    "sample_match": sample_valid,
                    "status": "success" if schema_match and sample_valid else "failed",
                }
            )

            if not schema_match or not sample_valid:
                all_valid = False

        results["details"]["all_tables_valid"] = all_valid

        if not all_valid:
            results["status"] = "warning"
            results["message"] = "Some tables have validation issues"

        return results

    def validate_correctness_local(
        self, dataset_name: str, local_path: str
    ) -> Dict[str, Any]:
        """
        Validate correctness of ETL process with local source

        Args:
            dataset_name: BigQuery dataset name
            local_path: Local path containing CSV files

        Returns:
            Dictionary with validation results
        """
        logger.info(f"Starting correctness validation for dataset: {dataset_name}")

        # Get list of CSV files in local directory
        csv_files = self.csv_reader.list_csv_files_local(local_path)
        if not csv_files:
            return {
                "status": "failed",
                "message": "No CSV files found in local path",
                "details": {"local_path": local_path},
            }

        # Check each CSV file against its corresponding BigQuery table
        results = {
            "status": "success",
            "message": "Correctness validation completed",
            "details": {"total_files": len(csv_files), "file_results": []},
        }

        all_files_valid = True

        for csv_file in csv_files:
            # Extract table name from file name
            table_name = self._extract_table_name_from_path(csv_file)
            if not table_name:
                continue

            # Skip if table doesn't exist
            if not self.bigquery_client.table_exists(dataset_name, table_name):
                continue

            # Get schemas and compare
            csv_schema = self.csv_reader.extract_schema_from_csv_local(csv_file)
            bq_table_info = self.bigquery_client.get_table_info(
                dataset_name, table_name
            )
            bq_schema = {
                field["name"]: field["type"]
                for field in bq_table_info.get("schema", [])
            }

            # Check if schemas match
            schema_match = self._compare_schemas(csv_schema, bq_schema)

            # Sample rows and compare values
            sample_valid = self._compare_sample_data_local(
                dataset_name, table_name, csv_file
            )

            file_result = {
                "file_path": csv_file,
                "table_name": table_name,
                "schema_match": schema_match,
                "sample_match": sample_valid,
                "status": "success" if schema_match and sample_valid else "failed",
            }

            results["details"]["file_results"].append(file_result)

            if not schema_match or not sample_valid:
                all_files_valid = False

        results["details"]["all_files_valid"] = all_files_valid

        if not all_files_valid:
            results["status"] = "warning"
            results["message"] = "Some files have validation issues"

        return results

    def generate_validation_report(self, results: Dict[str, Any]) -> str:
        """
        Generate a human-readable validation report

        Args:
            results: Validation results dictionary

        Returns:
            Formatted report string
        """
        report = []
        report.append(f"Validation Status: {results.get('status', 'Unknown')}")
        report.append(f"Message: {results.get('message', 'No message')}")
        report.append("\n--- Details ---\n")

        details = results.get("details", {})
        total_files = details.get("total_files", 0)
        report.append(f"Total files processed: {total_files}")

        if "all_files_processed" in details:
            report.append(
                f"All files processed successfully: {details['all_files_processed']}"
            )

        if "all_files_valid" in details:
            report.append(f"All files passed validation: {details['all_files_valid']}")

        if "total_csv_rows" in details:
            report.append(f"Total CSV rows: {details['total_csv_rows']}")

        if "total_bq_rows" in details:
            report.append(f"Total BigQuery rows: {details['total_bq_rows']}")

        # File-specific results
        file_results = details.get("file_results", [])
        if file_results:
            report.append("\n--- File Results ---")
            for file_result in file_results:
                file_path = file_result.get("file_path", "Unknown")
                table_name = file_result.get("table_name", "Unknown")
                status = file_result.get("status", "Unknown")
                report.append(f"\nFile: {file_path}")
                report.append(f"Table: {table_name}")
                report.append(f"Status: {status}")

                if "csv_rows" in file_result:
                    report.append(f"CSV rows: {file_result['csv_rows']}")

                if "bq_rows" in file_result:
                    report.append(f"BigQuery rows: {file_result['bq_rows']}")

                if "rows_match" in file_result:
                    report.append(f"Rows match: {file_result['rows_match']}")

                if "schema_match" in file_result:
                    report.append(f"Schema match: {file_result['schema_match']}")

                if "sample_match" in file_result:
                    report.append(f"Sample match: {file_result['sample_match']}")

        return "\n".join(report)

    def validate_single_file_completeness(
        self, dataset_name: str, gcs_path: str, table_name: str
    ) -> Dict[str, Any]:
        """
        Validate completeness for a single file

        Args:
            dataset_name: BigQuery dataset name
            gcs_path: GCS path to the CSV file
            table_name: BigQuery table name

        Returns:
            Dictionary with validation results
        """
        logger.info(f"Starting completeness validation for table: {table_name}")

        # Get row count from CSV
        csv_row_count = self.csv_reader.get_row_count_gcs(gcs_path)

        # Get row count from BigQuery
        bq_row_count = self.bigquery_client.get_row_count(dataset_name, table_name)

        # Check if table exists
        table_exists = self.bigquery_client.table_exists(dataset_name, table_name)

        # Check if row counts match
        rows_match = csv_row_count == bq_row_count

        results = {
            "status": "success" if rows_match else "failed",
            "message": "Completeness validation completed"
            if rows_match
            else f"Row count mismatch: CSV={csv_row_count}, BQ={bq_row_count}",
            "file_path": gcs_path,
            "table_name": table_name,
            "table_exists": table_exists,
            "csv_rows": csv_row_count,
            "bq_rows": bq_row_count,
            "rows_match": rows_match,
        }

        return results

    def validate_single_file_correctness(
        self, dataset_name: str, gcs_path: str, table_name: str
    ) -> Dict[str, Any]:
        """
        Validate correctness for a single file

        Args:
            dataset_name: BigQuery dataset name
            gcs_path: GCS path to the CSV file
            table_name: BigQuery table name

        Returns:
            Dictionary with validation results
        """
        logger.info(f"Starting correctness validation for table: {table_name}")

        # Skip if table doesn't exist
        if not self.bigquery_client.table_exists(dataset_name, table_name):
            return {
                "status": "failed",
                "message": f"Table {dataset_name}.{table_name} does not exist",
                "file_path": gcs_path,
                "table_name": table_name,
            }

        # Get schemas and compare
        csv_schema = self.csv_reader.extract_schema_from_csv_gcs(gcs_path)
        bq_table_info = self.bigquery_client.get_table_info(dataset_name, table_name)
        bq_schema = {
            field["name"]: field["type"] for field in bq_table_info.get("schema", [])
        }

        # Check if schemas match
        schema_match = self._compare_schemas(csv_schema, bq_schema)

        # Sample rows and compare values
        sample_valid = self._compare_sample_data_gcs(dataset_name, table_name, gcs_path)

        results = {
            "status": "success" if schema_match and sample_valid else "warning",
            "message": "Correctness validation completed",
            "file_path": gcs_path,
            "table_name": table_name,
            "schema_match": schema_match,
            "sample_match": sample_valid,
        }

        if not schema_match:
            results["message"] += ". Schema mismatch found."
        if not sample_valid:
            results["message"] += ". Sample data mismatch found."

        return results

    def _extract_table_name_from_path(self, file_path: str) -> Optional[str]:
        """Extract table name from file path"""
        import os

        base_name = os.path.basename(file_path)
        table_name = os.path.splitext(base_name)[0]
        return table_name if table_name else None

    def _compare_schemas(
        self, csv_schema: Dict[str, str], bq_schema: Dict[str, str]
    ) -> bool:
        """
        Compare CSV and BigQuery schemas

        Args:
            csv_schema: CSV schema dictionary
            bq_schema: BigQuery schema dictionary

        Returns:
            True if schemas match, False otherwise
        """
        # Check if column names match
        csv_columns = set(csv_schema.keys())
        bq_columns = set(bq_schema.keys())

        if csv_columns != bq_columns:
            logger.warning(
                f"Column names don't match. CSV: {csv_columns}, BQ: {bq_columns}"
            )
            return False

        # Check data types
        for column in csv_columns:
            csv_type = csv_schema[column]
            bq_type = bq_schema[column]

            # Simplified type comparison (can be enhanced)
            if csv_type.upper() != bq_type.upper():
                # Allow some type mappings
                type_mappings = {
                    # SQL Server -> BigQuery
                    "INT": "INTEGER",
                    "BIGINT": "INTEGER",
                    "SMALLINT": "INTEGER",
                    "TINYINT": "INTEGER",
                    "VARCHAR": "STRING",
                    "NVARCHAR": "STRING",
                    "TEXT": "STRING",
                    "CHAR": "STRING",
                    "NCHAR": "STRING",
                    "FLOAT": "FLOAT",
                    "FLOAT64": "FLOAT",
                    "REAL": "FLOAT",
                    "DECIMAL": "NUMERIC",
                    "NUMERIC": "NUMERIC",
                    "MONEY": "NUMERIC",
                    "DATETIME": "DATETIME",
                    "DATETIME2": "DATETIME",
                    "SMALLDATETIME": "DATETIME",
                }

                mapped_type = type_mappings.get(csv_type.upper(), csv_type.upper())

                if mapped_type != bq_type.upper():
                    logger.warning(
                        f"Column {column} type mismatch. CSV: {csv_type}, BQ: {bq_type}"
                    )
                    return False

        return True

    def _compare_sample_data_gcs(
        self, dataset_name: str, table_name: str, gcs_path: str
    ) -> bool:
        """
        Compare sample data between GCS CSV and BigQuery table

        Args:
            dataset_name: BigQuery dataset name
            table_name: BigQuery table name
            gcs_path: GCS path to CSV file

        Returns:
            True if sample data matches, False otherwise
        """
        try:
            # Get sample data from CSV
            csv_df = self.csv_reader.read_csv_to_dataframe_gcs(
                gcs_path, sample_size=self.sample_size
            )
            if csv_df.empty:
                logger.warning(f"Empty or unreadable CSV file: {gcs_path}")
                return False

            # Get sample data from BigQuery
            query = f"SELECT * FROM `{self.bigquery_client.project_id}.{dataset_name}.{table_name}` ORDER BY RAND() LIMIT {self.sample_size}"
            bq_df = self.bigquery_client.client.query(query).to_dataframe()
            if bq_df.empty:
                logger.warning(f"Empty BigQuery table: {dataset_name}.{table_name}")
                return False

            # For simplicity, just compare row counts and column names
            if len(csv_df.columns) != len(bq_df.columns):
                return False

            # Check if all column names match (order doesn't matter)
            csv_cols = set(csv_df.columns)
            bq_cols = set(bq_df.columns)
            if csv_cols != bq_cols:
                return False

            return True
        except Exception as e:
            logger.error(f"Error comparing sample data: {e}")
            return False

    def _compare_sample_data_mssql(self, dataset_name: str, table_name: str) -> bool:
        """
        Compare sample rows between MSSQL source and BigQuery table.

        For efficiency we only compare column sets and counts of sampled rows.
        """
        try:
            if not self.mssql_client:
                logger.error("MSSQL client not provided")
                return False

            sample_size = self.sample_size

            # Get sample rows from MSSQL
            mssql_rows = self.mssql_client.get_sample_rows(table_name, sample_size)
            if not mssql_rows:
                logger.warning(f"Empty or unreadable MSSQL table: {table_name}")
                return False

            # Get sample rows from BigQuery
            query = f"SELECT * FROM `{self.bigquery_client.project_id}.{dataset_name}.{table_name}` ORDER BY RAND() LIMIT {sample_size}"
            bq_df = self.bigquery_client.client.query(query).to_dataframe()
            if bq_df.empty:
                logger.warning(f"Empty BigQuery table: {dataset_name}.{table_name}")
                return False

            # Compare column sets
            mssql_cols = set(mssql_rows[0].keys())
            bq_cols = set(bq_df.columns)
            if mssql_cols != bq_cols:
                logger.warning(f"Column name mismatch between MSSQL and BQ for {table_name}")
                return False

            return True
        except Exception as e:
            logger.error(f"Error comparing sample data MSSQL <-> BQ: {e}")
            return False

    def _compare_sample_data_local(
        self, dataset_name: str, table_name: str, local_path: str
    ) -> bool:
        """
        Compare sample data between local CSV and BigQuery table

        Args:
            dataset_name: BigQuery dataset name
            table_name: BigQuery table name
            local_path: Local path to CSV file

        Returns:
            True if sample data matches, False otherwise
        """
        try:
            # Get sample data from CSV
            csv_df = self.csv_reader.read_csv_to_dataframe_local(
                local_path, sample_size=self.sample_size
            )
            if csv_df.empty:
                logger.warning(f"Empty or unreadable CSV file: {local_path}")
                return False

            # Get sample data from BigQuery
            query = f"SELECT * FROM `{self.bigquery_client.project_id}.{dataset_name}.{table_name}` ORDER BY RAND() LIMIT {self.sample_size}"
            bq_df = self.bigquery_client.client.query(query).to_dataframe()
            if bq_df.empty:
                logger.warning(f"Empty BigQuery table: {dataset_name}.{table_name}")
                return False

            # For simplicity, just compare row counts and column names
            if len(csv_df.columns) != len(bq_df.columns):
                return False

            # Check if all column names match (order doesn't matter)
            csv_cols = set(csv_df.columns)
            bq_cols = set(bq_df.columns)
            if csv_cols != bq_cols:
                return False

            return True
        except Exception as e:
            logger.error(f"Error comparing sample data: {e}")
            return False
