"""
Unit tests for BigQuery client module
"""

import unittest
from unittest.mock import MagicMock, patch, ANY

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
        # Ensure load job called with client-configured location
        mock_instance.load_table_from_uri.assert_called_once()
        mock_instance.load_table_from_uri.assert_called_once_with(
            gcs_uri, ANY, job_config=ANY, location=self.location
        )
        mock_load_job.result.assert_called_once()

    @patch("src.bigquery_client.bigquery.Client")
    def test_upsert_table_from_csv_new_table(self, mock_client):
        """Test upsert table from CSV when table doesn't exist"""
        # Setup mock client instance and BigQueryClient method patches
        mock_instance = MagicMock()
        mock_client.return_value = mock_instance

        # Patch BigQueryClient.table_exists to return False and create_table_from_csv to return True
        with patch("src.bigquery_client.BigQueryClient.table_exists", return_value=False), patch(
            "src.bigquery_client.BigQueryClient.create_table_from_csv", return_value=True
        ) as create_mock:
            # Create client
            bq_client = BigQueryClient(self.project_id, self.location)

            # Test upsert
            dataset_name = "test_dataset"
            table_name = "test_table"
            gcs_uri = "gs://test-bucket/test-file.csv"
            result = bq_client.upsert_table_from_csv(dataset_name, table_name, gcs_uri)

            # Verify
            self.assertTrue(result)
            create_mock.assert_called_once_with(dataset_name, table_name, gcs_uri, None, "WRITE_TRUNCATE")

    @patch("src.bigquery_client.bigquery.Client")
    def test_upsert_table_from_csv_existing_table_uses_dataset_location(
        self, mock_client
    ):
        """When target table exists, the temp table load and merge query should use dataset location"""
        mock_instance = MagicMock()
        mock_client.return_value = mock_instance

        # Simulate BigQueryClient.table_exists returning True
        with patch("src.bigquery_client.BigQueryClient.table_exists", return_value=True):
            # Simulate dataset location (same as client configuration to avoid enforcement abort)
            dataset_location = self.location
            dataset_obj = MagicMock()
            dataset_obj.location = dataset_location
            mock_instance.get_dataset.return_value = dataset_obj

            # target table schema (simple id + other col)
            schema_field_id = MagicMock()
            schema_field_id.name = "id"
            schema_field_other = MagicMock()
            schema_field_other.name = "name"

            mock_target_table = MagicMock()
            mock_target_table.schema = [schema_field_id, schema_field_other]
            mock_instance.get_table.return_value = mock_target_table

            # Ensure create_table_from_csv (on BigQueryClient) returns True
            with patch("src.bigquery_client.BigQueryClient.create_table_from_csv", return_value=True) as create_mock:

                # provide a query job mock
                mock_query_job = MagicMock()
                mock_query_job.result.return_value = None
                mock_instance.query.return_value = mock_query_job

                bq_client = BigQueryClient(self.project_id, self.location)

                dataset_name = "test_dataset"
                table_name = "test_table"
                gcs_uri = "gs://test-bucket/test-file.csv"

                result = bq_client.upsert_table_from_csv(dataset_name, table_name, gcs_uri)

                self.assertTrue(result)

                temp_table_name = f"{table_name}_temp"

                # create_table_from_csv should have been called for the temp table and passed dataset_location
                create_mock.assert_any_call(
                    dataset_name, temp_table_name, gcs_uri, None, "WRITE_TRUNCATE", location=dataset_location
                )

                # The merge query should be executed with the dataset location
                mock_instance.query.assert_called_once()
                # ensure it was called with location
                called_args, called_kwargs = mock_instance.query.call_args
                self.assertIn("location", called_kwargs)
                self.assertEqual(called_kwargs.get("location"), dataset_location)

    @patch("src.bigquery_client.bigquery.Client")
    def test_upsert_fails_on_dataset_location_mismatch_when_enforced(self, mock_client):
        """If dataset location mismatches and enforcement is enabled, upsert should abort"""
        mock_instance = MagicMock()
        mock_client.return_value = mock_instance

        with patch("src.bigquery_client.BigQueryClient.table_exists", return_value=True):
            # dataset reported location different than client
            dataset_obj = MagicMock()
            dataset_obj.location = "asia-northeast1"
            mock_instance.get_dataset.return_value = dataset_obj

            # patch create_table_from_csv so we can assert it is NOT called
            with patch("src.bigquery_client.BigQueryClient.create_table_from_csv", return_value=True) as create_mock:
                bq_client = BigQueryClient(self.project_id, self.location)

                dataset_name = "test_dataset"
                table_name = "test_table"
                gcs_uri = "gs://test-bucket/test-file.csv"

                # enforce_dataset_location True (default) -> should abort and return False
                result = bq_client.upsert_table_from_csv(dataset_name, table_name, gcs_uri, enforce_dataset_location=True)
                self.assertFalse(result)
                create_mock.assert_not_called()

    @patch("src.bigquery_client.bigquery.Client")
    def test_upsert_proceeds_when_enforcement_disabled(self, mock_client):
        """If dataset location mismatches but enforcement is disabled, upsert proceeds"""
        mock_instance = MagicMock()
        mock_client.return_value = mock_instance

        with patch("src.bigquery_client.BigQueryClient.table_exists", return_value=True):
            # dataset reported location different than client
            dataset_obj = MagicMock()
            dataset_obj.location = "asia-northeast1"
            mock_instance.get_dataset.return_value = dataset_obj

            # target table schema (simple id + other col)
            schema_field_id = MagicMock()
            schema_field_id.name = "id"
            schema_field_other = MagicMock()
            schema_field_other.name = "name"

            mock_target_table = MagicMock()
            mock_target_table.schema = [schema_field_id, schema_field_other]
            mock_instance.get_table.return_value = mock_target_table

            # patch create_table_from_csv to succeed
            with patch("src.bigquery_client.BigQueryClient.create_table_from_csv", return_value=True) as create_mock:
                mock_query_job = MagicMock()
                mock_query_job.result.return_value = None
                mock_instance.query.return_value = mock_query_job

                bq_client = BigQueryClient(self.project_id, self.location)

                dataset_name = "test_dataset"
                table_name = "test_table"
                gcs_uri = "gs://test-bucket/test-file.csv"

                result = bq_client.upsert_table_from_csv(dataset_name, table_name, gcs_uri, enforce_dataset_location=False)
                self.assertTrue(result)

                temp_table_name = f"{table_name}_temp"
                # create should have been called for temp with dataset's location
                create_mock.assert_any_call(dataset_name, temp_table_name, gcs_uri, None, "WRITE_TRUNCATE", location=dataset_obj.location)

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
