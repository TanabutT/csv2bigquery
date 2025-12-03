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

    def test_initialize_clients_with_secret_manager(self):
        config = {
            "project_id": "p1",
            "gcs_bucket": "b",
            "service_account_path": "/tmp/sa.json",
            "mssql": {
                "secret_name": "projects/810737581373/secrets/mssql-conn-string",
                "database": "mydb",
            },
        }

        # Make fake google.cloud.secretmanager module
        import sys
        import types

        secretmanager_mod = types.ModuleType("google.cloud.secretmanager")

        class FakeClient:
            def access_secret_version(self, name):
                payload = types.SimpleNamespace()
                payload.data = b"DRIVER=...;SERVER=...;Database=mydb;UID=u;PWD=p;"
                return types.SimpleNamespace(payload=payload)

        secretmanager_mod.SecretManagerServiceClient = lambda: FakeClient()

        # insert into sys.modules so initialize_clients import works
        google_mod = types.ModuleType("google")
        google_cloud_mod = types.ModuleType("google.cloud")
        # attach the submodule
        google_mod.cloud = google_cloud_mod
        google_cloud_mod.secretmanager = secretmanager_mod
        sys.modules["google"] = google_mod
        sys.modules["google.cloud"] = google_cloud_mod
        sys.modules["google.cloud.secretmanager"] = secretmanager_mod

        # Provide a fake mssql_client module so we don't attempt real connections
        fake_mod = MagicMock()
        fake_mod.MSSQLClient = MagicMock(return_value=MagicMock())
        sys.modules["mssql_client"] = fake_mod

        with patch("src.main.BigQueryClient") as mock_bq, patch("src.main.CSVReader") as mock_csv:
            mock_bq.return_value = MagicMock()
            mock_csv.return_value = MagicMock()
            bq, csv_reader, mssql_client = main.initialize_clients(config)

        # ensure MSSQLClient was called with connection_string argument
        fake_mod.MSSQLClient.assert_called()
        kwargs = fake_mod.MSSQLClient.call_args.kwargs
        # connection_string should be present and decoded string
        self.assertIn("connection_string", kwargs)
        self.assertTrue("DRIVER=" in kwargs["connection_string"])

        # cleanup inserted modules
        del sys.modules["mssql_client"]
        del sys.modules["google.cloud.secretmanager"]
        del sys.modules["google.cloud"]
        del sys.modules["google"]

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
