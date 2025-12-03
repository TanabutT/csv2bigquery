"""
Unit tests for main.py MSSQL integration
"""
import unittest
from unittest.mock import patch, MagicMock
import json
from src import main


class TestMainMSSQL(unittest.TestCase):
    def test_initialize_clients_with_mssql_config(self):
        config = {
            "project_id": "p1",
            "gcs_bucket": "b",
            "service_account_path": "/tmp/sa.json",
            "mssql": {
                "server": "sql.example",
                "database": "mydb",
                "username": "u",
                "password": "p",
            },
        }

        # Ensure that import of top-level 'mssql_client' module is mocked so
        # initialize_clients will import it without loading the real module
        import sys
        fake_mod = MagicMock()
        fake_mod.MSSQLClient = MagicMock(return_value=MagicMock())
        sys.modules["mssql_client"] = fake_mod

        # Patch BigQueryClient and CSVReader so initialization doesn't need real resources
        with patch("src.main.BigQueryClient") as mock_bq, patch("src.main.CSVReader") as mock_csv:
            mock_bq.return_value = MagicMock()
            mock_csv.return_value = MagicMock()
            bq, csv_reader, mssql_client = main.initialize_clients(config)
        # clean up fake module
        del sys.modules["mssql_client"]
        self.assertIsNotNone(bq)
        self.assertIsNotNone(csv_reader)
        self.assertIsNotNone(mssql_client)

    def test_initialize_clients_without_mssql_config(self):
        config = {
            "project_id": "p1",
            "gcs_bucket": "b",
            "service_account_path": "/tmp/sa.json",
        }
        with patch("src.main.BigQueryClient") as mock_bq, patch("src.main.CSVReader") as mock_csv:
            mock_bq.return_value = MagicMock()
            mock_csv.return_value = MagicMock()
            bq, csv_reader, mssql_client = main.initialize_clients(config)
        self.assertIsNotNone(bq)
        self.assertIsNotNone(csv_reader)
        self.assertIsNone(mssql_client)


if __name__ == "__main__":
    unittest.main()
