"""
Unit tests for MSSQL-based validator methods in validator_mssql.py
"""
import unittest
from unittest.mock import MagicMock, patch

from src.validator_mssql import Validator


class TestValidatorMSSQL(unittest.TestCase):
    def setUp(self):
        self.project_id = "test-project"
        # BigQuery client mock
        self.bq_client = MagicMock()
        self.bq_client.project_id = self.project_id

        # MSSQL client mock
        self.mssql_client = MagicMock()

        # create Validator with MSSQL client
        self.validator = Validator(self.bq_client, None, mssql_client=self.mssql_client)

    def test_validate_completeness_mssql_all_match(self):
        # MSSQL has two tables
        self.mssql_client.list_tables.return_value = ["table1", "table2"]
        # Row counts match in both sides
        self.mssql_client.get_row_count.side_effect = [10, 5]
        self.bq_client.get_row_count.side_effect = [10, 5]
        self.bq_client.table_exists.return_value = True

        res = self.validator.validate_completeness_mssql("dataset", "db")
        self.assertEqual(res["status"], "success")
        self.assertTrue(res["details"]["all_tables_match"])

    def test_validate_completeness_mssql_mismatch(self):
        self.mssql_client.list_tables.return_value = ["table1"]
        self.mssql_client.get_row_count.return_value = 12
        self.bq_client.get_row_count.return_value = 10
        self.bq_client.table_exists.return_value = True

        res = self.validator.validate_completeness_mssql("dataset", "db")
        self.assertEqual(res["status"], "warning")
        self.assertFalse(res["details"]["all_tables_match"])

    def test_validate_correctness_mssql_schema_and_sample_match(self):
        # One table present
        tables = ["tableA"]
        self.mssql_client.list_tables.return_value = tables
        # MSSQL schema
        self.mssql_client.get_table_schema.return_value = {"id": "int", "name": "varchar"}

        # BigQuery schema info
        self.bq_client.table_exists.return_value = True
        self.bq_client.get_table_info.return_value = {
            "schema": [{"name": "id", "type": "INT"}, {"name": "name", "type": "STRING"}]
        }

        # MSSQL sample rows
        self.mssql_client.get_sample_rows.return_value = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]

        # BigQuery returns pandas dataframe with same columns when queried
        mock_df = MagicMock()
        mock_df.empty = False
        mock_df.columns = ["id", "name"]
        self.bq_client.client.query.return_value.to_dataframe.return_value = mock_df

        res = self.validator.validate_correctness_mssql("dataset", "db")
        self.assertEqual(res["status"], "success")
        self.assertTrue(res["details"]["all_tables_valid"]) 

    def test_validate_correctness_mssql_schema_mismatch(self):
        tables = ["tableA"]
        self.mssql_client.list_tables.return_value = tables
        self.mssql_client.get_table_schema.return_value = {"id": "int", "name": "varchar"}

        self.bq_client.table_exists.return_value = True
        # BQ schema missing 'name'
        self.bq_client.get_table_info.return_value = {"schema": [{"name": "id", "type": "INT"}]}

        # If sample retrieval still returns data, overall validation should fail due to schema mismatch
        self.mssql_client.get_sample_rows.return_value = [{"id": 1, "name": "Alice"}]
        mock_df = MagicMock()
        mock_df.empty = False
        mock_df.columns = ["id", "name"]
        self.bq_client.client.query.return_value.to_dataframe.return_value = mock_df

        res = self.validator.validate_correctness_mssql("dataset", "db")
        self.assertEqual(res["status"], "warning")
        self.assertFalse(res["details"]["all_tables_valid"]) 


if __name__ == "__main__":
    unittest.main()
