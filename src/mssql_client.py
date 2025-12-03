import pyodbc
import logging
from typing import List, Optional, Any, Dict
from os import getenv

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class MSSQLClient:
    def __init__(
        self,
        connection_string: Optional[str] = None,
        driver: str = "{ODBC Driver 17 for SQL Server}",
        timeout: int = 30,
    ):
        """
        MSSQL Client for connecting to SQL Server and executing queries.

        Args:
            server: SQL Server hostname or IP address
            database: Default database name
            username: SQL authentication username
            password: SQL authentication password
            driver: ODBC driver
            timeout: Connection timeout in seconds
        """
    
        # This client prefers an explicit ODBC-style connection string. If the
        # caller supplies the connection_string parameter it will be used, or
        # the class will attempt to read SQL_CONNECTION_STRING from the
        # environment (for example loaded from a .env file). We no longer
        # assemble a connection string from individual server/username/password
        # components to keep the configuration explicit and reduce accidental
        # insecure defaults.
        self.connection_string = connection_string
        self.driver = driver
        self.timeout = timeout

        self.cnxn = None
        # Do not eagerly open a connection -- attempt to connect lazily
        # caller can also call connect() explicitly or use test_connection()

    # --------------------------------------------------------
    # DB CONNECTION
    # --------------------------------------------------------
    def connect(self) -> bool:
        """Attempt to establish a connection.

        Returns True if the connection succeeds, False otherwise. Uses the
        explicitly-provided connection_string if given, otherwise will look for
        SQL_CONNECTION_STRING in the environment and fall back to assembled
        parameters.
        """
        try:
            # Require an explicit connection string: either supplied to the
            # constructor or available via SQL_CONNECTION_STRING in the
            # environment (e.g., a .env file). This avoids implicit or
            # partially-specified connection data.
            conn_str = self.connection_string or getenv("SQL_CONNECTION_STRING")

            if not conn_str:
                # Fail fast and avoid attempting to assemble a connection
                # string from pieces — the caller should supply a complete ODBC
                # connection string.
                logger.error(
                    "No MSSQL connection string provided. Set `connection_string` or SQL_CONNECTION_STRING env var."
                )
                return False

            logger.info("Connecting to SQL Server...")
            # pass timeout to pyodbc; it's also honored in the connection string
            self.cnxn = pyodbc.connect(conn_str, timeout=self.timeout)
            logger.info("SQL Server connection established")
            return True

        except Exception as e:
            logger.error(f"Failed to connect to SQL Server: {e}")
            self.cnxn = None
            return False

    def _execute_query(self, query: str, params: Optional[List[Any]] = None):
        """
        Execute a SQL query safely with optional parameters.
        """
        if not self.cnxn:
            connected = self.connect()
            if not connected:
                raise RuntimeError("No active DB connection and connect() failed")

        try:
            cursor = self.cnxn.cursor()
            logger.debug("Executing query", extra={"query": query})
            cursor.execute(query) if not params else cursor.execute(query, params)
            return cursor.fetchall()
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            raise

    # --------------------------------------------------------
    # TABLE LISTING
    # --------------------------------------------------------
    def list_tables(self, database_name: str) -> List[str]:
        """
        Return list of table names in a SQL Server database.
        """
        query = f"""
        SELECT TABLE_NAME
        FROM {database_name}.INFORMATION_SCHEMA.TABLES
        WHERE TABLE_TYPE = 'BASE TABLE'
        ORDER BY TABLE_NAME;
        """

        rows = self._execute_query(query)
        tables = [row[0] for row in rows]

        logger.info(f"Found {len(tables)} tables in SQL Server database {database_name}")
        return tables

    # --------------------------------------------------------
    # ROW COUNT PER TABLE
    # --------------------------------------------------------
    def get_row_count(self, table_name: str) -> int:
        """
        Return number of rows in a SQL Server table.
        """
        query = f"SELECT COUNT(*) FROM [{table_name}]"

        try:
            rows = self._execute_query(query)
            row_count = rows[0][0] if rows else 0
            logger.info(f"Row count for table {table_name}: {row_count}")
            return row_count
        except Exception as e:
            logger.error(f"Failed to get row count for table {table_name}: {e}")
            return 0

    def get_table_schema(self, table_name: str) -> Dict[str, str]:
        """
        Return column names and data types for a SQL Server table.

        Returns:
            Dict mapping column name -> data type (as string)
        """
        query = f"""
        SELECT COLUMN_NAME, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = '{table_name}'
        ORDER BY ORDINAL_POSITION
        """

        try:
            rows = self._execute_query(query)
            schema = {row[0]: row[1] for row in rows}
            logger.info(f"Schema for {table_name}: {schema}")
            return schema
        except Exception as e:
            logger.error(f"Failed to get schema for {table_name}: {e}")
            return {}

    def get_sample_rows(self, table_name: str, sample_size: int = 100) -> Optional[list]:
        """
        Return a sample of rows from the table as list of dicts.

        On SQL Server, ORDER BY NEWID() is a common way to randomize rows.
        """
        try:
            # First get columns
            schema = self.get_table_schema(table_name)
            if not schema:
                return None
            cols = ", ".join([f"[{c}]" for c in schema.keys()])
            query = f"SELECT TOP ({sample_size}) {cols} FROM [{table_name}] ORDER BY NEWID()"
            if not self.cnxn:
                connected = self.connect()
                if not connected:
                    return None

            cursor = self.cnxn.cursor()
            cursor.execute(query)
            rows = cursor.fetchall()
            col_names = [desc[0] for desc in cursor.description]
            # Convert to list of dicts
            result = [dict(zip(col_names, row)) for row in rows]
            return result
        except Exception as e:
            logger.error(f"Failed to fetch sample rows for {table_name}: {e}")
            return None

    def test_connection(self, test_query: str = "SELECT 1") -> bool:
        """
        Perform a simple connectivity test against the MSSQL server.

        Returns True if the client can successfully execute the test_query, False otherwise.
        """
        # Ensure connected and run a tiny query
        if not self.cnxn:
            ok = self.connect()
            if not ok:
                logger.error("MSSQL connectivity test failed: unable to connect")
                return False

        try:
            rows = self._execute_query(test_query)
            if not rows:
                logger.warning("MSSQL test query returned no rows")
                return False

            logger.info("MSSQL connectivity test succeeded")
            return True

        except Exception as e:
            logger.error(f"MSSQL connectivity test failed: {e}")
            return False
