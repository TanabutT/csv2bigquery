import pyodbc
import logging
from typing import List, Optional, Any, Dict

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class MSSQLClient:
    def __init__(
        self,
        server: str,
        database: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
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
        self.server = server
        self.database = database
        self.username = username
        self.password = password
        self.driver = driver
        self.timeout = timeout

        self.cnxn = None
        self._connect()

    # --------------------------------------------------------
    # DB CONNECTION
    # --------------------------------------------------------
    def _connect(self):
        try:
            conn_str = (
                f"DRIVER={self.driver};"
                f"SERVER={self.server};"
                f"Database={self.database};"
                f"UID={self.username};"
                f"PWD={self.password};"
                f"TrustServerCertificate=yes;"
                f"Connection Timeout={self.timeout};"
            )

            logger.info(f"Connecting to SQL Server at: {self.server}")
            self.cnxn = pyodbc.connect(conn_str)
            logger.info("SQL Server connection established successfully")

        except Exception as e:
            logger.error(f"Failed to connect to SQL Server: {e}")
            raise

    def _execute_query(self, query: str, params: Optional[List[Any]] = None):
        """
        Execute a SQL query safely with optional parameters.
        """
        try:
            cursor = self.cnxn.cursor()
            logger.debug(f"Executing query: {query}, params: {params}")
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
