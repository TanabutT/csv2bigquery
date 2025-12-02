"""
Unit tests for CSV reader module
"""

import unittest
from unittest.mock import MagicMock, mock_open, patch

import pandas as pd

from src.CSV_reader import CSVReader


class TestCSVReader(unittest.TestCase):
    """Test cases for CSVReader class"""

    def setUp(self):
        """Set up test fixtures"""
        self.gcs_bucket = "test-bucket"
        self.service_account_path = "/path/to/service-account.json"

    @patch("src.CSV_reader.storage.Client")
    def test_init_with_gcs(self, mock_client):
        """Test initialization with GCS bucket"""
        CSVReader(self.gcs_bucket, self.service_account_path)
        mock_client.from_service_account_json.assert_called_once_with(
            self.service_account_path
        )

    @patch("src.CSV_reader.storage.Client")
    def test_init_without_gcs(self, mock_client):
        """Test initialization without GCS bucket"""
        CSVReader()
        mock_client.assert_not_called()

    @patch("src.CSV_reader.storage.Client")
    def test_list_csv_files_in_gcs(self, mock_client):
        """Test listing CSV files in GCS"""
        # Setup mock
        mock_instance = MagicMock()
        mock_bucket = MagicMock()
        mock_blob1 = MagicMock()
        mock_blob1.name = "file1.csv"
        mock_blob2 = MagicMock()
        mock_blob2.name = "folder/file2.csv"
        mock_blob3 = MagicMock()
        mock_blob3.name = "file3.txt"

        mock_bucket.list_blobs.return_value = [mock_blob1, mock_blob2, mock_blob3]
        mock_instance.bucket.return_value = mock_bucket
        mock_client.return_value = mock_instance

        # Create reader
        csv_reader = CSVReader(self.gcs_bucket)

        # Test listing
        gcs_path = "test-folder"
        result = csv_reader.list_csv_files_in_gcs(gcs_path)

        # Verify
        self.assertEqual(len(result), 2)
        self.assertIn("file1.csv", result)
        self.assertIn("folder/file2.csv", result)

    @patch("src.CSV_reader.os.walk")
    def test_list_csv_files_local(self, mock_walk):
        """Test listing CSV files in local directory"""
        # Setup mock
        mock_walk.return_value = [
            ("/test/dir", ["subdir"], ["file1.csv", "file2.txt"]),
            ("/test/dir/subdir", [], ["file2.csv", "file3.csv"]),
        ]

        # Create reader
        csv_reader = CSVReader()

        # Test listing
        directory_path = "/test/dir"
        result = csv_reader.list_csv_files_local(directory_path)

        # Verify
        self.assertEqual(len(result), 3)
        self.assertIn("/test/dir/file1.csv", result)
        self.assertIn("/test/dir/subdir/file2.csv", result)
        self.assertIn("/test/dir/subdir/file3.csv", result)

    @patch("src.CSV_reader.storage.Client")
    def test_get_csv_metadata_from_gcs(self, mock_client):
        """Test getting CSV metadata from GCS"""
        # Setup mock
        mock_instance = MagicMock()
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        mock_blob.name = "test-file.csv"
        mock_blob.size = 1024
        mock_blob.time_created.isoformat.return_value = "2023-01-01T00:00:00"
        mock_blob.updated.isoformat.return_value = "2023-01-01T00:00:00"

        mock_bucket.list_blobs.return_value = [mock_blob]
        mock_bucket.blob.return_value = mock_blob
        mock_instance.bucket.return_value = mock_bucket
        mock_client.return_value = mock_instance

        # Create reader
        csv_reader = CSVReader(self.gcs_bucket)

        # Test getting metadata
        gcs_path = "test-folder"
        result = csv_reader.get_csv_metadata_from_gcs(gcs_path)

        # Verify
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["name"], "test-file.csv")
        self.assertEqual(result[0]["size"], 1024)

    @patch("src.CSV_reader.os.stat")
    @patch("src.CSV_reader.os.walk")
    def test_get_csv_metadata_local(self, mock_walk, mock_stat):
        """Test getting CSV metadata from local directory"""
        # Setup mock
        mock_walk.return_value = [
            ("/test/dir", [], ["file1.csv"]),
        ]

        mock_stat_result = MagicMock()
        mock_stat_result.st_size = 1024
        mock_stat_result.st_ctime = 1672531200  # 2023-01-01 00:00:00
        mock_stat_result.st_mtime = 1672531200  # 2023-01-01 00:00:00
        mock_stat.return_value = mock_stat_result

        # Create reader
        csv_reader = CSVReader()

        # Test getting metadata
        directory_path = "/test/dir"
        result = csv_reader.get_csv_metadata_local(directory_path)

        # Verify
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["name"], "file1.csv")
        self.assertEqual(result[0]["size"], 1024)

    @patch("src.CSV_reader.storage.Client")
    @patch("src.CSV_reader.pd.read_csv")
    def test_extract_schema_from_csv_gcs(self, mock_read_csv, mock_client):
        """Test extracting schema from GCS CSV"""
        # Setup mock
        mock_df = pd.DataFrame(
            {
                "col1": [1, 2, 3],
                "col2": ["a", "b", "c"],
                "col3": [1.1, 2.2, 3.3],
                "col4": [True, False, True],
            }
        )
        mock_read_csv.return_value = mock_df

        mock_blob = MagicMock()
        mock_blob.download_as_bytes.return_value = (
            b"col1,col2,col3,col4\n1,a,1.1,True\n"
        )

        mock_instance = MagicMock()
        mock_bucket = MagicMock()
        mock_bucket.blob.return_value = mock_blob
        mock_instance.bucket.return_value = mock_bucket
        mock_client.return_value = mock_instance

        # Create reader
        csv_reader = CSVReader(self.gcs_bucket)

        # Test schema extraction
        gcs_path = "test-file.csv"
        result = csv_reader.extract_schema_from_csv_gcs(gcs_path)

        # Verify
        self.assertEqual(result["col1"], "INTEGER")
        self.assertEqual(result["col2"], "STRING")
        self.assertEqual(result["col3"], "FLOAT")
        self.assertEqual(result["col4"], "BOOLEAN")

    @patch("src.CSV_reader.pd.read_csv")
    def test_extract_schema_from_csv_local(self, mock_read_csv):
        """Test extracting schema from local CSV"""
        # Setup mock
        mock_df = pd.DataFrame(
            {
                "col1": [1, 2, 3],
                "col2": ["a", "b", "c"],
                "col3": [1.1, 2.2, 3.3],
                "col4": [True, False, True],
            }
        )
        mock_read_csv.return_value = mock_df

        # Create reader
        csv_reader = CSVReader()

        # Test schema extraction
        file_path = "/test/file.csv"
        result = csv_reader.extract_schema_from_csv_local(file_path)

        # Verify
        self.assertEqual(result["col1"], "INTEGER")
        self.assertEqual(result["col2"], "STRING")
        self.assertEqual(result["col3"], "FLOAT")
        self.assertEqual(result["col4"], "BOOLEAN")

    @patch("src.CSV_reader.storage.Client")
    @patch("src.CSV_reader.pd.read_csv")
    def test_read_csv_to_dataframe_gcs(self, mock_read_csv, mock_client):
        """Test reading CSV from GCS to DataFrame"""
        # Setup mock
        mock_df = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
        mock_read_csv.return_value = mock_df

        # Create reader
        csv_reader = CSVReader(self.gcs_bucket)

        # Test reading
        gcs_path = "test-file.csv"
        result = csv_reader.read_csv_to_dataframe_gcs(gcs_path)

        # Verify
        self.assertEqual(len(result), 3)
        self.assertIn("col1", result.columns)
        self.assertIn("col2", result.columns)
        mock_read_csv.assert_called_once()

    @patch("src.CSV_reader.pd.read_csv")
    @patch("builtins.open", new_callable=mock_open, read_data="col1,col2\n1,a\n2,b\n")
    def test_read_csv_to_dataframe_local(self, mock_read_csv, mock_file):
        """Test reading local CSV to DataFrame"""
        # Setup mock
        mock_df = pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})
        mock_read_csv.return_value = mock_df

        # Create reader
        csv_reader = CSVReader()

        # Test reading
        file_path = "/test/file.csv"
        result = csv_reader.read_csv_to_dataframe_local(file_path)

        # Verify
        self.assertEqual(len(result), 2)
        self.assertIn("col1", result.columns)
        self.assertIn("col2", result.columns)
        mock_read_csv.assert_called_once_with(file_path, low_memory=False)

    @patch("src.CSV_reader.storage.Client")
    @patch("src.CSV_reader.pd.read_csv")
    def test_get_row_count_gcs(self, mock_read_csv, mock_client):
        """Test getting row count from GCS CSV"""
        # Setup mock
        mock_df = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
        mock_read_csv.return_value = mock_df

        # Create reader
        csv_reader = CSVReader(self.gcs_bucket)

        # Test getting row count
        gcs_path = "test-file.csv"
        result = csv_reader.get_row_count_gcs(gcs_path)

        # Verify
        self.assertEqual(result, 3)

    @patch("src.CSV_reader.pd.read_csv")
    @patch("builtins.open", new_callable=mock_open, read_data="col1,col2\n1,a\n2,b\n")
    def test_get_row_count_local(self, mock_read_csv, mock_file):
        """Test getting row count from local CSV"""
        # Setup mock
        mock_df = pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})
        mock_read_csv.return_value = mock_df

        # Create reader
        csv_reader = CSVReader()

        # Test getting row count
        file_path = "/test/file.csv"
        result = csv_reader.get_row_count_local(file_path)

        # Verify
        self.assertEqual(result, 2)


if __name__ == "__main__":
    unittest.main()
