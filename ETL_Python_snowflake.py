import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

# Step 1: Extract
def extract_csv(file_path):
    return pd.read_csv(file_path)

# Step 2: Transform
def transform_data(df):
    # Example transformation: drop rows with missing values
    df = df.dropna()
    return df

# Step 3: Load
def load_to_snowflake(df, table_name, snowflake_connection_params):
    # Create a connection to Snowflake
    conn = snowflake.connector.connect(
        user=snowflake_connection_params['user'],
        password=snowflake_connection_params['password'],
        account=snowflake_connection_params['account'],
        warehouse=snowflake_connection_params['warehouse'],
        database=snowflake_connection_params['database'],
        schema=snowflake_connection_params['schema']
    )
    
    # Load DataFrame to Snowflake
    success, nchunks, nrows, _ = write_pandas(conn, df, table_name)
    conn.close()
    
    if success:
        print(f"Successfully loaded {nrows} rows to {table_name} in {nchunks} chunks.")
    else:
        print("Failed to load data to Snowflake.")

# ETL Pipeline
def etl_pipeline(file_path, table_name, snowflake_connection_params):
    data = extract_csv(file_path)
    transformed_data = transform_data(data)
    load_to_snowflake(transformed_data, table_name, snowflake_connection_params)

# Example usage
file_path = 'data.csv'
table_name = 'TRANSFORMED_DATA'
snowflake_connection_params = {
    'user': 'YOUR_USER',
    'password': 'YOUR_PASSWORD',
    'account': 'YOUR_ACCOUNT',
    'warehouse': 'YOUR_WAREHOUSE',
    'database': 'YOUR_DATABASE',
    'schema': 'YOUR_SCHEMA'
}

etl_pipeline(file_path, table_name, snowflake_connection_params)