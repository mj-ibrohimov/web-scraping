import sqlite3
import pandas as pd

# Function to create a connection to the database
def create_connection(db_name):
    conn = None
    try:
        conn = sqlite3.connect(db_name)
        print(f"Connection to {db_name} established.")
    except sqlite3.Error as e:
        print(f"An error occurred: {e}")
    return conn

# Function to write a DataFrame to the database
def save_to_database(df, table_name, conn):
    try:
        df.to_sql(table_name, conn, if_exists='replace', index=False)
        print(f"Data successfully written to {table_name}.")
    except Exception as e:
        print(f"An error occurred while writing to the database: {e}")

# Function to read data from the database
def read_from_database(table_name, conn):
    """Read data from the specified table in the database and return as DataFrame."""
    try:
        query_result = pd.read_sql(f"SELECT * FROM {table_name}", conn)
        return query_result
    except Exception as e:
        print(f"An error occurred while reading from the database: {e}")
        return None
