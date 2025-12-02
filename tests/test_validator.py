"""
Unit tests for validator module
"""

import unittest
from unittest.mock import MagicMock, patch

from src.validator import Validator


class TestValidator(unittest.TestCase):
    """Test cases for Validator class"""

    def setUp(self):
        """Set up test fixtures"""
        # Create mock clients
        self.mock_bq_client = MagicMock()
        self.mock_csv_reader = MagicMock()

        # Create validator instance
        self.validator = Validator(self.mock_bq_client, self.mock_csv_reader)

    def test_init(self):
        """Test validator initialization"""
        self.assertEqual(self.validator.bigquery_client, self.mock_bq_client)
        self.assertEqual(self.validator.csv_reader, self.mock_csv_reader)
        self.assertEqual(self.validator.sample_size, 100)  # Default sample size

    def test_init_with_custom_sample_size(self):
        """Test validator initialization with custom sample size"""
        validator = Validator(
            self.mock_bq_client, self.mock_csv_reader, sample_size=200
        )
        self.assertEqual(validator.sample_size, 200)

    def test_validate_completeness_gcs_success(self):
        """Test successful completeness validation for GCS source"""
        # Setup mocks
        self.mock_csv_reader.list_csv_files_in_gcs.return_value = [
            "file1.csv",
            "file2.csv",
        ]
        self.mock_csv_reader.get_row_count_gcs.side_effect = [100, 200]
        self.mock_bq_client.get_row_count.side_effect = [100, 200]
        self.mock_bq_client.table_exists.side_effect = [True, True]

        # Test validation
        result = self.validator.validate_completeness_gcs("test_dataset", "test_path")

        # Verify
        self.assertEqual(result["status"], "success")
        self.assertEqual(result["details"]["total_files"], 2)
        self.assertEqual(result["details"]["all_files_processed"], True)
        self.assertEqual(result["details"]["total_csv_rows"], 300)
        self.assertEqual(result["details"]["total_bq_rows"], 300)
        self.assertEqual(len(result["details"]["file_results"]), 2)

    def test_validate_completeness_gcs_no_files(self):
        """Test completeness validation for GCS source with no files"""
        # Setup mocks
        self.mock_csv_reader.list_csv_files_in_gcs.return_value = []

        # Test validation
        result = self.validator.validate_completeness_gcs("test_dataset", "test_path")

        # Verify
        self.assertEqual(result["status"], "failed")
        self.assertEqual(result["message"], "No CSV files found in GCS path")

    def test_validate_completeness_gcs_row_mismatch(self):
        """Test completeness validation for GCS source with row count mismatches"""
        # Setup mocks
        self.mock_csv_reader.list_csv_files_in_gcs.return_value = [
            "file1.csv",
            "file2.csv",
        ]
        self.mock_csv_reader.get_row_count_gcs.side_effect = [100, 200]
        self.mock_bq_client.get_row_count.side_effect = [100, 150]  # Mismatch for file2
        self.mock_bq_client.table_exists.side_effect = [True, True]

        # Test validation
        result = self.validator.validate_completeness_gcs("test_dataset", "test_path")

        # Verify
        self.assertEqual(result["status"], "warning")
        self.assertEqual(result["message"], "Some files have row count mismatches")
        self.assertEqual(result["details"]["all_files_processed"], False)
        self.assertEqual(result["details"]["file_results"][0]["rows_match"], True)
        self.assertEqual(result["details"]["file_results"][1]["rows_match"], False)

    def test_validate_completeness_local_success(self):
        """Test successful completeness validation for local source"""
        # Setup mocks
        self.mock_csv_reader.list_csv_files_local.return_value = [
            "file1.csv",
            "file2.csv",
        ]
        self.mock_csv_reader.get_row_count_local.side_effect = [100, 200]
        self.mock_bq_client.get_row_count.side_effect = [100, 200]
        self.mock_bq_client.table_exists.side_effect = [True, True]

        # Test validation
        result = self.validator.validate_completeness_local("test_dataset", "test_path")

        # Verify
        self.assertEqual(result["status"], "success")
        self.assertEqual(result["details"]["total_files"], 2)
        self.assertEqual(result["details"]["all_files_processed"], True)
        self.assertEqual(result["details"]["total_csv_rows"], 300)
        self.assertEqual(result["details"]["total_bq_rows"], 300)
        self.assertEqual(len(result["details"]["file_results"]), 2)

    def test_validate_correctness_gcs_success(self):
        """Test successful correctness validation for GCS source"""
        # Setup mocks
        self.mock_csv_reader.list_csv_files_in_gcs.return_value = ["file1.csv"]
        self.mock_bq_client.table_exists.return_value = True
        self.mock_csv_reader.extract_schema_from_csv_gcs.return_value = {
            "col1": "STRING"
        }
        self.mock_bq_client.get_table_info.return_value = {
            "schema": [{"name": "col1", "type": "STRING"}]
        }
        self.mock_csv_reader.read_csv_to_dataframe_gcs.return_value = MagicMock()
        self.mock_bq_client.client.query.return_value.to_dataframe.return_value = (
            MagicMock()
        )

        # Test validation
        result = self.validator.validate_correctness_gcs("test_dataset", "test_path")

        # Verify
        self.assertEqual(result["status"], "success")
        self.assertEqual(result["details"]["all_files_valid"], True)
        self.assertEqual(len(result["details"]["file_results"]), 1)
        self.assertEqual(result["details"]["file_results"][0]["schema_match"], True)

    def test_validate_correctness_local_success(self):
        """Test successful correctness validation for local source"""
        # Setup mocks
        self.mock_csv_reader.list_csv_files_local.return_value = ["file1.csv"]
        self.mock_bq_client.table_exists.return_value = True
        self.mock_csv_reader.extract_schema_from_csv_local.return_value = {
            "col1": "STRING"
        }
        self.mock_bq_client.get_table_info.return_value = {
            "schema": [{"name": "col1", "type": "STRING"}]
        }
        self.mock_csv_reader.read_csv_to_dataframe_local.return_value = MagicMock()
        self.mock_bq_client.client.query.return_value.to_dataframe.return_value = (
            MagicMock()
        )

        # Test validation
        result = self.validator.validate_correctness_local("test_dataset", "test_path")

        # Verify
        self.assertEqual(result["status"], "success")
        self.assertEqual(result["details"]["all_files_valid"], True)
        self.assertEqual(len(result["details"]["file_results"]), 1)
        self.assertEqual(result["details"]["file_results"][0]["schema_match"], True)

    def test_compare_schemas_match(self):
        """Test schema comparison when schemas match"""
        csv_schema = {"col1": "STRING", "col2": "INTEGER"}
        bq_schema = {"col1": "STRING", "col2": "INTEGER"}

        result = self.validator._compare_schemas(csv_schema, bq_schema)
        self.assertTrue(result)

    def test_compare_schemas_column_mismatch(self):
        """Test schema comparison when column names don't match"""
        csv_schema = {"col1": "STRING", "col2": "INTEGER"}
        bq_schema = {"col1": "STRING", "col3": "INTEGER"}

        result = self.validator._compare_schemas(csv_schema, bq_schema)
        self.assertFalse(result)

    def test_compare_schemas_type_mismatch(self):
        """Test schema comparison when data types don't match"""
        csv_schema = {"col1": "STRING", "col2": "INTEGER"}
        bq_schema = {"col1": "STRING", "col2": "FLOAT"}

        result = self.validator._compare_schemas(csv_schema, bq_schema)
        self.assertFalse(result)

    def test_compare_schemas_type_mapping(self):
        """Test schema comparison with type mappings"""
        csv_schema = {"col1": "STRING", "col2": "INT"}
        bq_schema = {"col1": "STRING", "col2": "INTEGER"}

        result = self.validator._compare_schemas(csv_schema, bq_schema)
        self.assertTrue(result)

    def test_extract_table_name_from_path(self):
        """Test extracting table name from file path"""
        # Test with simple path
        result = self.validator._extract_table_name_from_path("test_table.csv")
        self.assertEqual(result, "test_table")

        # Test with directory path
        result = self.validator._extract_table_name_from_path("/path/to/test_table.csv")
        self.assertEqual(result, "test_table")

        # Test with GCS path
        result = self.validator._extract_table_name_from_path(
            "gs://bucket/path/to/test_table.csv"
        )
        self.assertEqual(result, "test_table")

    def test_generate_validation_report(self):
        """Test generation of validation report"""
        # Create sample results
        results = {
            "status": "warning",
            "message": "Some files failed validation",
            "details": {
                "total_files": 2,
                "all_files_valid": False,
                "total_csv_rows": 300,
                "total_bq_rows": 250,
                "file_results": [
                    {
                        "file_path": "file1.csv",
                        "table_name": "table1",
                        "status": "success",
                        "csv_rows": 100,
                        "bq_rows": 100,
                        "rows_match": True,
                    },
                    {
                        "file_path": "file2.csv",
                        "table_name": "table2",
                        "status": "failed",
                        "csv_rows": 200,
                        "bq_rows": 150,
                        "rows_match": False,
                    },
                ],
            },
        }

        # Generate report
        report = self.validator.generate_validation_report(results)

        # Verify
        self.assertIn("Validation Status: warning", report)
        self.assertIn("Message: Some files failed validation", report)
        self.assertIn("Total files processed: 2", report)
        self.assertIn("Total CSV rows: 300", report)
        self.assertIn("Total BigQuery rows: 250", report)
        self.assertIn("File: file1.csv", report)
        self.assertIn("Table: table1", report)
        self.assertIn("Status: success", report)
        self.assertIn("CSV rows: 100", report)
        self.assertIn("BigQuery rows: 100", report)
        self.assertIn("Rows match: True", report)

    def test_compare_sample_data_gcs_success(self):
        """Test successful sample data comparison for GCS"""
        # Setup mocks
        mock_csv_df = MagicMock()
        mock_csv_df.columns = ["col1", "col2"]
        mock_csv_df.empty = False
        self.mock_csv_reader.read_csv_to_dataframe_gcs.return_value = mock_csv_df

        mock_bq_df = MagicMock()
        mock_bq_df.columns = ["col1", "col2"]
        mock_bq_df.empty = False
        self.mock_bq_client.client.query.return_value.to_dataframe.return_value = (
            mock_bq_df
        )

        # Test comparison
        result = self.validator._compare_sample_data_gcs(
            "test_dataset", "test_table", "test_path"
        )

        # Verify
        self.assertTrue(result)
        self.mock_csv_reader.read_csv_to_dataframe_gcs.assert_called_once()
        self.mock_bq_client.client.query.assert_called_once()

    def test_compare_sample_data_gcs_empty_csv(self):
        """Test sample data comparison for GCS with empty CSV"""
        # Setup mocks
        mock_csv_df = MagicMock()
        mock_csv_df.empty = True
        self.mock_csv_reader.read_csv_to_dataframe_gcs.return_value = mock_csv_df

        # Test comparison
        result = self.validator._compare_sample_data_gcs(
            "test_dataset", "test_table", "test_path"
        )

        # Verify
        self.assertFalse(result)
        self.mock_csv_reader.read_csv_to_dataframe_gcs.assert_called_once()

    def test_compare_sample_data_local_success(self):
        """Test successful sample data comparison for local files"""
        # Setup mocks
        mock_csv_df = MagicMock()
        mock_csv_df.columns = ["col1", "col2"]
        mock_csv_df.empty = False
        self.mock_csv_reader.read_csv_to_dataframe_local.return_value = mock_csv_df

        mock_bq_df = MagicMock()
        mock_bq_df.columns = ["col1", "col2"]
        mock_bq_df.empty = False
        self.mock_bq_client.client.query.return_value.to_dataframe.return_value = (
            mock_bq_df
        )

        # Test comparison
        result = self.validator._compare_sample_data_local(
            "test_dataset", "test_table", "test_path"
        )

        # Verify
        self.assertTrue(result)
        self.mock_csv_reader.read_csv_to_dataframe_local.assert_called_once()
        self.mock_bq_client.client.query.assert_called_once()

    def test_compare_sample_data_column_mismatch(self):
        """Test sample data comparison with column mismatch"""
        # Setup mocks
        mock_csv_df = MagicMock()
        mock_csv_df.columns = ["col1", "col2"]  # Different columns
        mock_csv_df.empty = False
        self.mock_csv_reader.read_csv_to_dataframe_gcs.return_value = mock_csv_df

        mock_bq_df = MagicMock()
        mock_bq_df.columns = ["col1", "col3"]  # Different columns
        mock_bq_df.empty = False
        self.mock_bq_client.client.query.return_value.to_dataframe.return_value = (
            mock_bq_df
        )

        # Test comparison
        result = self.validator._compare_sample_data_gcs(
            "test_dataset", "test_table", "test_path"
        )

        # Verify
        self.assertFalse(result)


if __name__ == "__main__":
    unittest.main()
