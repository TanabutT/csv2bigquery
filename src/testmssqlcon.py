# connection to mssql server with python and this conn string 
"""
Connects to a SQL database using mssql-python
"""

from os import getenv 
from dotenv import load_dotenv 
import pyodbc # Make sure you import pyodbc

load_dotenv()  # take environment variables from .env.

# The entire connection string is read from the environment variable as one string
connection_string = getenv("SQL_CONNECTION_STRING") 

# Pass the single string to the connect function
conn = pyodbc.connect(connection_string) 
print("connection successful")
# ... rest of your code ...
