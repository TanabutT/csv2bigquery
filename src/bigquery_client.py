"""
BigQuery client module for connecting to and managing BigQuery resources
"""

import logging
from typing import Any, Dict, List, Optional

from google.api_core.exceptions import GoogleAPIError
from google.cloud import bigquery

logger = logging.getLogger(__name__)


class BigQueryClient:
    """Client for interacting with Google BigQuery"""

    def __init__(
        self, project_id: str, location: str, service_account_path: Optional[str] = None
    ):
        """
        Initialize BigQuery client

        Args:
            project_id: GCP project ID
            location: Dataset location/region
            service_account_path: Path to service account key file (optional)
        """
        self.project_id = project_id
        self.location = location

        # Initialize client with authentication
        if service_account_path:
            self.client = bigquery.Client.from_service_account_json(
                service_account_path, project=project_id
            )
        else:
            # Use default credentials
            self.client = bigquery.Client(project=project_id)

        # Increase connection pool size to handle parallel operations
        import os

        if "GOOGLE_CLOUD_CONNECTION_POOL_SIZE" in os.environ:
            pool_size = int(os.environ["GOOGLE_CLOUD_CONNECTION_POOL_SIZE"])
        else:
            pool_size = 50  # Increase default to 50 to handle parallel operations

        self.client._http_connection_pool_size = pool_size

        logger.info(f"BigQuery client initialized for project: {project_id}")

    def create_dataset(self, dataset_name: str, exists_ok: bool = True) -> bool:
        """
        Create a dataset if it doesn't exist

        Args:
            dataset_name: Name of the dataset to create
            exists_ok: If True, don't raise an error if dataset already exists

        Returns:
            True if dataset was created or already exists, False otherwise
        """
        try:
            dataset_ref = self.client.dataset(dataset_name)
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = self.location

            dataset = self.client.create_dataset(dataset, exists_ok=exists_ok)
            logger.info(f"Dataset {dataset_name} created or already exists")
            return True
        except GoogleAPIError as e:
            logger.error(f"Error creating dataset {dataset_name}: {e}")
            return False

    def table_exists(self, dataset_name: str, table_name: str) -> bool:
        """
        Check if a table exists in BigQuery

        Args:
            dataset_name: Name of the dataset
            table_name: Name of the table

        Returns:
            True if table exists, False otherwise
        """
        try:
            from google.cloud.exceptions import NotFound

            dataset_ref = self.client.dataset(dataset_name)
            table_ref = dataset_ref.table(table_name)
            try:
                self.client.get_table(table_ref)
                return True
            except NotFound:
                return False
        except Exception as e:
            logger.error(f"Error checking if table exists: {e}")
            return False

    def create_table_from_csv(
        self,
        dataset_name: str,
        table_name: str,
        gcs_uri: str,
        schema: Optional[List[bigquery.SchemaField]] = None,
        write_disposition: str = "WRITE_TRUNCATE",
    ) -> bool:
        """
        Create a table in BigQuery from a CSV file in GCS

        Args:
            dataset_name: Name of the dataset
            table_name: Name of the table to create
            gcs_uri: GCS URI of the CSV file
            schema: Table schema (optional, will auto-detect if None)
            write_disposition: WRITE_TRUNCATE, WRITE_APPEND, or WRITE_EMPTY

        Returns:
            True if table was created successfully, False otherwise
        """
        try:
            dataset_ref = self.client.dataset(dataset_name)
            table_ref = dataset_ref.table(table_name)

            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.CSV,
                skip_leading_rows=1,  # Skip header row
                autodetect=True if schema is None else False,
                schema=schema,
                write_disposition=write_disposition,
                # max_bad_records=1,  # Allow some errors
                allow_quoted_newlines=True,

            )

            load_job = self.client.load_table_from_uri(
                gcs_uri, table_ref, job_config=job_config, location="asia-southeast1",
            )

            load_job.result()  # Wait for job to complete

            logger.info(f"Table {dataset_name}.{table_name} created from {gcs_uri}")
            return True
        except GoogleAPIError as e:
            logger.error(f"Error creating table {dataset_name}.{table_name}: {e}")
            return False

    def get_table_info(self, dataset_name: str, table_name: str) -> Dict[str, Any]:
        """
        Get table schema and metadata

        Args:
            dataset_name: Name of the dataset
            table_name: Name of the table

        Returns:
            Dictionary containing table information
        """
        try:
            dataset_ref = self.client.dataset(dataset_name)
            table_ref = dataset_ref.table(table_name)
            table = self.client.get_table(table_ref)

            return {
                "table_id": table.table_id,
                "dataset_id": table.dataset_id,
                "project": table.project,
                "num_rows": table.num_rows,
                "num_bytes": table.num_bytes,
                "created": table.created.isoformat() if table.created else None,
                "modified": table.modified.isoformat() if table.modified else None,
                "schema": [
                    {"name": field.name, "type": field.field_type, "mode": field.mode}
                    for field in table.schema
                ],
            }
        except GoogleAPIError as e:
            logger.error(
                f"Error getting table info for {dataset_name}.{table_name}: {e}"
            )
            return {}

    def upsert_table_from_csv(
        self,
        dataset_name: str,
        table_name: str,
        gcs_uri: str,
        temp_table_suffix: str = "_temp",
        schema: Optional[List[bigquery.SchemaField]] = None,
    ) -> bool:
        """
        Upsert (update or insert) data from CSV into existing table

        Args:
            dataset_name: Name of the dataset
            table_name: Name of the target table
            gcs_uri: GCS URI of the CSV file
            temp_table_suffix: Suffix for temporary table
            schema: Table schema (optional)

        Returns:
            True if upsert was successful, False otherwise
        """
        try:
            # Check if target table exists
            table_exists = self.table_exists(dataset_name, table_name)

            if not table_exists:
                # If table doesn't exist, just create it
                return self.create_table_from_csv(
                    dataset_name, table_name, gcs_uri, schema, "WRITE_TRUNCATE"
                )

            # If table exists, create a temporary table with new data
            temp_table_name = f"{table_name}{temp_table_suffix}"

            # Load new data into temp table
            if not self.create_table_from_csv(
                dataset_name, temp_table_name, gcs_uri, schema, "WRITE_TRUNCATE"
            ):
                return False

            # Get schema to determine primary key (assume first field as key for this example)
            # In a real implementation, you'd determine this more robustly
            target_table = self.client.get_table(
                self.client.dataset(dataset_name).table(table_name)
            )

            if not target_table.schema:
                logger.error(f"Target table {dataset_name}.{table_name} has no schema")
                return False

            # Find a suitable primary key (prefer id or first field)
            primary_key = None
            for field in target_table.schema:
                if field.name.lower() == "id":
                    primary_key = field.name
                    break

            if not primary_key:
                primary_key = target_table.schema[0].name

            # Check if temp table exists before proceeding
            try:
                temp_table = self.client.get_table(
                    self.client.dataset(dataset_name).table(temp_table_name)
                )
                if temp_table:
                    # Delete any existing temp table first
                    self.client.delete_table(
                        self.client.dataset(dataset_name).table(temp_table_name)
                    )
            except:
                # Table doesn't exist, which is fine
                pass

            # Construct SQL for upsert with better handling of duplicates
            sql = f"""
            MERGE `{self.project_id}.{dataset_name}.{table_name}` AS target
            USING `{self.project_id}.{dataset_name}.{temp_table_name}` AS source
            ON target.{primary_key} = source.{primary_key}
            WHEN MATCHED THEN
              UPDATE SET {
                ", ".join(
                    [
                        f"target.{field.name} = source.{field.name}"
                        for field in target_table.schema
                        if field.name != primary_key
                    ]
                )
            }
            WHEN NOT MATCHED THEN
              INSERT ({", ".join([field.name for field in target_table.schema])})
              VALUES ({
                ", ".join(["source." + field.name for field in target_table.schema])
            })
            """

            query_job = self.client.query(sql)
            query_job.result()

            # Delete temporary table
            self.client.delete_table(
                self.client.dataset(dataset_name).table(temp_table_name)
            )

            logger.info(f"Upsert completed for {dataset_name}.{table_name}")
            return True
        except GoogleAPIError as e:
            logger.error(f"Error during upsert for {dataset_name}.{table_name}: {e}")
            return False

    def get_row_count(self, dataset_name: str, table_name: str) -> int:
        """
        Get row count for a table

        Args:
            dataset_name: Name of the dataset
            table_name: Name of the table

        Returns:
            Number of rows in the table
        """
        try:
            query = f"SELECT COUNT(*) as count FROM `{self.project_id}.{dataset_name}.{table_name}`"
            query_job = self.client.query(query)
            results = query_job.result()

            for row in results:
                return int(row.count)

            return 0
        except GoogleAPIError as e:
            logger.error(
                f"Error getting row count for {dataset_name}.{table_name}: {e}"
            )
            return 0
