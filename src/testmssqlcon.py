from mssql_client import *

mssql_client = MSSQLClient()._connect()
#Execute a small test query
rows = MSSQLClient()._execute_query("SELECT * FROM [dbo].[test]")

if rows is None:
    print("MSSQL test query returned no rows")
    

print("MSSQL connectivity test succeeded")
    