import os
import pyodbc

# Get environment variables
DB_SERVER = os.getenv('DB_SERVER', 'host.docker.internal')  # Default value if not found
DB_USER = os.getenv('DB_USER', 'sa')  # Default value if not found
DB_PASSWORD = os.getenv('DB_PASSWORD', 'YourStrong!Passw0rd')  # Default value if not found
DB_NAME = os.getenv('DB_NAME', 'ighd')  # Default value if not found

# Database connection configuration
connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={DB_SERVER};DATABASE={DB_NAME};UID={DB_USER};PWD={DB_PASSWORD};Encrypt=no;TrustServerCertificate=yes'

# Establish the connection using pyodbc
try:
    conn = pyodbc.connect(connection_string)
    print("Connected to the database successfully!")
except Exception as e:
    print(f"Error connecting to the database: {e}")
