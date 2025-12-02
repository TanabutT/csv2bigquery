"""
Unit tests for BigQuery client module
"""

import unittest
from unittest.mock import MagicMock, patch

from src.bigquery_client import BigQueryClient


class TestBigQueryClient(unittest.TestCase):
    """Test cases for BigQueryClient class"""

    def setUp(self):
        """Set up test fixtures"""
        self.project_id = "test-project"
        self.location = "us-central1"
        self.service_account_path = "/path/to/service-account.json"

    @patch("src.bigquery_client.bigquery.Client")
    def test_init_with_service_account(self, mock_client):
        """Test initialization with service account"""
        BigQueryClient(self.project_id, self.location, self.service_account_path)
        mock_client.from_service_account_json.assert_called_once_with(
            self.service_account_path, project=self.project_id
        )

    @patch("src.bigquery_client.bigquery.Client")
    def test_init_without_service_account(self, mock_client):
        """Test initialization without service account"""
        BigQueryClient(self.project_id, self.location)
        mock_client.assert_called_once_with(project=self.project_id)

    @patch("src.bigquery_client.bigquery.Client")
    def test_create_dataset(self, mock_client):
        """Test dataset creation"""
        # Setup mock
        mock_instance = MagicMock()
        mock_client.return_value = mock_instance
        mock_instance.create_dataset.return_value = MagicMock()

        # Create client
        bq_client = BigQueryClient(self.project_id, self.location)

        # Test dataset creation
        dataset_name = "test_dataset"
        result = bq_client.create_dataset(dataset_name)

        # Verify
        self.assertTrue(result)
        mock_instance.create_dataset.assert_called_once()

    @patch("src.bigquery_client.bigquery.Client")
    def test_table_exists_true(self, mock_client):
        """Test table exists check when table exists"""
        # Setup mock
        mock_instance = MagicMock()
        mock_client.return_value = mock_instance
        mock_instance.get_table.return_value = MagicMock()

        # Create client
        bq_client = BigQueryClient(self.project_id, self.location)

        # Test table exists
        dataset_name = "test_dataset"
        table_name = "test_table"
        result = bq_client.table_exists(dataset_name, table_name)

        # Verify
        self.assertTrue(result)
        mock_instance.get_table.assert_called_once()

    @patch("src.bigquery_client.bigquery.Client")
    def test_table_exists_false(self, mock_client):
        """Test table exists check when table doesn't exist"""
        # Setup mock
        from google.api_core.exceptions import GoogleAPIError

        mock_instance = MagicMock()
        mock_client.return_value = mock_instance
        mock_instance.get_table.side_effect = GoogleAPIError("Not found: Table")

        # Create client
        bq_client = BigQueryClient(self.project_id, self.location)

        # Test table exists
        dataset_name = "test_dataset"
        table_name = "test_table"
        result = bq_client.table_exists(dataset_name, table_name)

        # Verify
        self.assertFalse(result)

    @patch("src.bigquery_client.bigquery.Client")
    def test_create_table_from_csv(self, mock_client):
        """Test table creation from CSV"""
        # Setup mock
        mock_instance = MagicMock()
        mock_load_job = MagicMock()
        mock_load_job.result.return_value = None
        mock_instance.load_table_from_uri.return_value = mock_load_job
        mock_client.return_value = mock_instance

        # Create client
        bq_client = BigQueryClient(self.project_id, self.location)

        # Test table creation
        dataset_name = "test_dataset"
        table_name = "test_table"
        gcs_uri = "gs://test-bucket/test-file.csv"
        result = bq_client.create_table_from_csv(dataset_name, table_name, gcs_uri)

        # Verify
        self.assertTrue(result)
        mock_instance.load_table_from_uri.assert_called_once()
        mock_load_job.result.assert_called_once()

    @patch("src.bigquery_client.bigquery.Client")
    def test_upsert_table_from_csv_new_table(self, mock_client):
        """Test upsert table from CSV when table doesn't exist"""
        # Setup mock
        mock_instance = MagicMock()
        mock_instance.table_exists.return_value = False
        mock_instance.create_table_from_csv.return_value = True
        mock_client.return_value = mock_instance

        # Create client
        bq_client = BigQueryClient(self.project_id, self.location)

        # Test upsert
        dataset_name = "test_dataset"
        table_name = "test_table"
        gcs_uri = "gs://test-bucket/test-file.csv"
        result = bq_client.upsert_table_from_csv(dataset_name, table_name, gcs_uri)

        # Verify
        self.assertTrue(result)
        mock_instance.table_exists.assert_called_once_with(dataset_name, table_name)
        mock_instance.create_table_from_csv.assert_called_once_with(
            dataset_name, table_name, gcs_uri, None, "WRITE_TRUNCATE"
        )

    @patch("src.bigquery_client.bigquery.Client")
    def test_get_table_info(self, mock_client):
        """Test getting table information"""
        # Setup mock
        mock_table = MagicMock()
        mock_table.table_id = "test_table"
        mock_table.dataset_id = "test_dataset"
        mock_table.project = "test-project"
        mock_table.num_rows = 100
        mock_table.num_bytes = 1000
        mock_table.created = "2023-01-01T00:00:00"
        mock_table.modified = "2023-01-01T00:00:00"
        mock_table.schema = [
            MagicMock(name="col1", field_type="STRING", mode="NULLABLE"),
            MagicMock(name="col2", field_type="INTEGER", mode="NULLABLE"),
        ]
        mock_schema_field = MagicMock()
        mock_schema_field.name = "col1"
        mock_schema_field.field_type = "STRING"
        mock_schema_field.mode = "NULLABLE"
        mock_table.schema = [mock_schema_field]

        mock_instance = MagicMock()
        mock_instance.get_table.return_value = mock_table
        mock_client.return_value = mock_instance

        # Create client
        bq_client = BigQueryClient(self.project_id, self.location)

        # Test get table info
        dataset_name = "test_dataset"
        table_name = "test_table"
        info = bq_client.get_table_info(dataset_name, table_name)

        # Verify
        self.assertEqual(info["table_id"], "test_table")
        self.assertEqual(info["dataset_id"], "test_dataset")
        self.assertEqual(info["project"], "test-project")
        self.assertEqual(info["num_rows"], 100)
        self.assertEqual(len(info["schema"]), 1)
        self.assertEqual(info["schema"][0]["name"], "col1")


if __name__ == "__main__":
    unittest.main()
