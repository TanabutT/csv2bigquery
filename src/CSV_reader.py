"""
CSV reader module for processing CSV files from Google Cloud Storage
"""

import logging
import os
from typing import Dict, List, Optional, Tuple

import pandas as pd
from google.cloud import storage

logger = logging.getLogger(__name__)


class CSVReader:
    """Reader for processing CSV files from GCS or local filesystem"""

    def __init__(
        self,
        gcs_bucket: Optional[str] = None,
        service_account_path: Optional[str] = None,
    ):
        """
        Initialize CSV Reader

        Args:
            gcs_bucket: GCS bucket name (optional)
            service_account_path: Path to service account key file (optional)
        """
        self.gcs_bucket = gcs_bucket

        # Initialize GCS client if bucket is provided
        if gcs_bucket:
            if service_account_path:
                self.gcs_client = storage.Client.from_service_account_json(
                    service_account_path
                )
            else:
                self.gcs_client = storage.Client()
            self.bucket = self.gcs_client.bucket(gcs_bucket)
            logger.info(f"GCS client initialized for bucket: {gcs_bucket}")
        else:
            self.gcs_client = None
            self.bucket = None
            logger.info("CSV Reader initialized for local filesystem")

    def list_csv_files_in_gcs(self, gcs_path: str) -> List[str]:
        """
        List all CSV files in a GCS path

        Args:
            gcs_path: GCS path (folder) to search for CSV files

        Returns:
            List of CSV file paths in GCS
        """
        if not self.gcs_client:
            logger.error("GCS client not initialized")
            return []

        try:
            # Ensure path doesn't start with a slash
            if gcs_path.startswith("/"):
                gcs_path = gcs_path[1:]

            blobs = self.bucket.list_blobs(prefix=gcs_path)
            csv_files = [
                blob.name
                for blob in blobs
                if blob.name.lower().endswith(".csv")
                and "prisma" not in blob.name.lower()
            ]

            logger.info(f"Found {len(csv_files)} CSV files in GCS path: {gcs_path}")
            return csv_files
        except Exception as e:
            logger.error(f"Error listing CSV files in GCS path {gcs_path}: {e}")
            return []

    def list_csv_files_local(self, directory_path: str) -> List[str]:
        """
        List all CSV files in a local directory

        Args:
            directory_path: Local directory path to search for CSV files

        Returns:
            List of CSV file paths
        """
        try:
            csv_files = []
            for root, _, files in os.walk(directory_path):
                for file in files:
                    if file.lower().endswith(".csv") and "prisma" not in file.lower():
                        csv_files.append(os.path.join(root, file))

            logger.info(
                f"Found {len(csv_files)} CSV files in local directory: {directory_path}"
            )
            return csv_files
        except Exception as e:
            logger.error(
                f"Error listing CSV files in local directory {directory_path}: {e}"
            )
            return []

    def get_csv_metadata_from_gcs(self, gcs_path: str) -> List[Dict[str, str]]:
        """
        Get metadata for CSV files in GCS

        Args:
            gcs_path: GCS path (folder) containing CSV files

        Returns:
            List of dictionaries with file metadata
        """
        if not self.gcs_client:
            logger.error("GCS client not initialized")
            return []

        try:
            csv_files = self.list_csv_files_in_gcs(gcs_path)
            metadata = []

            for file_path in csv_files:
                blob = self.bucket.blob(file_path)
                blob.reload()  # Fetch the latest metadata

                metadata.append(
                    {
                        "path": file_path,
                        "name": os.path.basename(file_path),
                        "size": blob.size,
                        "created": blob.time_created.isoformat()
                        if blob.time_created
                        else None,
                        "updated": blob.updated.isoformat() if blob.updated else None,
                    }
                )

            return metadata
        except Exception as e:
            logger.error(f"Error getting CSV metadata from GCS path {gcs_path}: {e}")
            return []

    def get_csv_metadata_local(self, directory_path: str) -> List[Dict[str, str]]:
        """
        Get metadata for CSV files in a local directory

        Args:
            directory_path: Local directory containing CSV files

        Returns:
            List of dictionaries with file metadata
        """
        try:
            csv_files = self.list_csv_files_local(directory_path)
            metadata = []

            for file_path in csv_files:
                stat = os.stat(file_path)
                metadata.append(
                    {
                        "path": file_path,
                        "name": os.path.basename(file_path),
                        "size": stat.st_size,
                        "created": stat.st_ctime,
                        "updated": stat.st_mtime,
                    }
                )

            return metadata
        except Exception as e:
            logger.error(
                f"Error getting CSV metadata from local directory {directory_path}: {e}"
            )
            return []

    def extract_schema_from_csv_gcs(
        self, gcs_path: str, sample_size: int = 1000
    ) -> Dict[str, str]:
        """
        Extract schema from a CSV file in GCS by sampling a few rows

        Args:
            gcs_path: GCS path to the CSV file
            sample_size: Number of rows to sample for schema detection

        Returns:
            Dictionary with column names as keys and inferred data types as values
        """
        if not self.gcs_client:
            logger.error("GCS client not initialized")
            return {}

        try:
            # Download a small portion of the file for schema detection
            blob = self.bucket.blob(gcs_path)

            # Download the file content as bytes
            content = blob.download_as_bytes()

            # Use pandas to infer schema
            from io import StringIO

            df = pd.read_csv(
                StringIO(content.decode("utf-8")),
                nrows=sample_size,
                low_memory=False,
            )

            schema = {}
            for column in df.columns:
                dtype = df[column].dtype
                if pd.api.types.is_integer_dtype(dtype):
                    schema[column] = "INTEGER"
                elif pd.api.types.is_float_dtype(dtype):
                    schema[column] = "FLOAT"
                elif pd.api.types.is_bool_dtype(dtype):
                    schema[column] = "BOOLEAN"
                elif pd.api.types.is_datetime64_dtype(dtype):
                    schema[column] = "TIMESTAMP"
                else:
                    schema[column] = "STRING"

            logger.info(f"Extracted schema for {gcs_path}: {schema}")
            return schema
        except Exception as e:
            logger.error(f"Error extracting schema from {gcs_path}: {e}")
            return {}

    def extract_schema_from_csv_local(
        self, file_path: str, sample_size: int = 1000
    ) -> Dict[str, str]:
        """
        Extract schema from a local CSV file by sampling a few rows

        Args:
            file_path: Local path to the CSV file
            sample_size: Number of rows to sample for schema detection

        Returns:
            Dictionary with column names as keys and inferred data types as values
        """
        try:
            # Use pandas to infer schema
            df = pd.read_csv(
                file_path,
                nrows=sample_size,
                low_memory=False,
            )

            schema = {}
            for column in df.columns:
                dtype = df[column].dtype
                if pd.api.types.is_integer_dtype(dtype):
                    schema[column] = "INTEGER"
                elif pd.api.types.is_float_dtype(dtype):
                    schema[column] = "FLOAT"
                elif pd.api.types.is_bool_dtype(dtype):
                    schema[column] = "BOOLEAN"
                elif pd.api.types.is_datetime64_dtype(dtype):
                    schema[column] = "TIMESTAMP"
                else:
                    schema[column] = "STRING"

            logger.info(f"Extracted schema for {file_path}: {schema}")
            return schema
        except Exception as e:
            logger.error(f"Error extracting schema from {file_path}: {e}")
            return {}

    def read_csv_to_dataframe_gcs(
        self, gcs_path: str, sample_size: Optional[int] = None
    ) -> pd.DataFrame:
        """
        Read a CSV file from GCS into a pandas DataFrame

        Args:
            gcs_path: GCS path to the CSV file
            sample_size: Optional number of rows to sample (if provided)

        Returns:
            pandas DataFrame with the CSV data
        """
        if not self.gcs_client:
            logger.error("GCS client not initialized")
            return pd.DataFrame()

        try:
            # Download the file content as bytes using the GCS client
            blob = self.bucket.blob(gcs_path)
            content = blob.download_as_bytes()

            # Read with pandas from bytes
            from io import StringIO

            if sample_size:
                df = pd.read_csv(
                    StringIO(content.decode("utf-8")),
                    nrows=sample_size,
                    low_memory=False,
                )
            else:
                df = pd.read_csv(StringIO(content.decode("utf-8")), low_memory=False)

            logger.info(f"Read CSV from GCS: {gcs_path}, shape: {df.shape}")
            return df
        except Exception as e:
            logger.error(f"Error reading CSV from GCS {gcs_path}: {e}")
            return pd.DataFrame()

    def read_csv_to_dataframe_local(
        self, file_path: str, sample_size: Optional[int] = None
    ) -> pd.DataFrame:
        """
        Read a local CSV file into a pandas DataFrame

        Args:
            file_path: Local path to the CSV file
            sample_size: Optional number of rows to sample (if provided)

        Returns:
            pandas DataFrame with the CSV data
        """
        try:
            # Read with pandas
            if sample_size:
                df = pd.read_csv(file_path, nrows=sample_size, low_memory=False)
            else:
                df = pd.read_csv(file_path, low_memory=False)

            logger.info(f"Read local CSV: {file_path}, shape: {df.shape}")
            return df
        except Exception as e:
            logger.error(f"Error reading local CSV {file_path}: {e}")
            return pd.DataFrame()

    def get_row_count_gcs(self, gcs_path: str) -> int:
        """
        Get the row count of a CSV file in GCS

        Args:
            gcs_path: GCS path to the CSV file

        Returns:
            Number of rows in the CSV file
        """
        df = self.read_csv_to_dataframe_gcs(gcs_path)
        return len(df)

    def get_row_count_local(self, file_path: str) -> int:
        """
        Get the row count of a local CSV file

        Args:
            file_path: Local path to the CSV file

        Returns:
            Number of rows in the CSV file
        """
        df = self.read_csv_to_dataframe_local(file_path)
        return len(df)
