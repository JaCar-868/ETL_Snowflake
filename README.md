# ETL_Snowflake
This repository contains a sample ETL (Extract, Transform, Load) pipeline using Python, SQL, and Spark. The project demonstrates how to extract data from various sources, transform it, and load it into a Snowflake data warehouse.

## Project Description

The ETL pipeline involves the following steps:
1. **Extract**: Read data from a CSV file.
2. **Transform**: Process the data using Pandas.
3. **Load**: Insert the transformed data into a Snowflake table.

## Prerequisites

To run this project, you need the following:
- A Snowflake account.
- Python 3.6 or higher.
- Required Python libraries: `pandas`, `snowflake-connector-python`.
- Access to a PostgreSQL database (for the SQL example).
- Spark installation (for the Spark example).

## Setup

1. Clone the repository:
   (open command line tool)
   
   git clone https://github.com/yourusername/etl-pipeline-project.git
   cd etl-pipeline-project
   
2. Install the required Python libraries:

pip install pandas snowflake-connector-python

3. Set up your Snowflake connection parameters in a config.json file:

{
    "user": "YOUR_USER",
    "password": "YOUR_PASSWORD",
    "account": "YOUR_ACCOUNT",
    "warehouse": "YOUR_WAREHOUSE",
    "database": "YOUR_DATABASE",
    "schema": "YOUR_SCHEMA"
}

## Usage
Python-Based ETL Pipeline
This pipeline extracts data from a CSV file, transforms it using Pandas, and loads it into a Snowflake table.

1. Modify the etl_pipeline.py script with the path to your CSV file and table name:

file_path = 'data.csv'
table_name = 'TRANSFORMED_DATA'

2. Run the ETL pipeline:

python etl_pipeline.py

## SQL-Based ETL Pipeline
This pipeline extracts data from one PostgreSQL table, transforms it using SQL queries, and loads it into another PostgreSQL table.

Run the SQL queries in your PostgreSQL database:

-- Extract
CREATE TABLE extracted_data AS
SELECT * FROM source_table;

-- Transform
CREATE TABLE transformed_data AS
SELECT 
    id, 
    UPPER(name) AS name, 
    date_of_birth 
FROM extracted_data
WHERE date_of_birth IS NOT NULL;

-- Load
INSERT INTO target_table
SELECT * FROM transformed_data;

## Spark-Based ETL Pipeline
This pipeline extracts data from a JSON file, transforms it using PySpark, and loads it into HDFS.

1. Modify the spark_etl_pipeline.py script with the path to your JSON file and HDFS output path:

file_path = 'data.json'
output_path = 'hdfs:///user/hadoop/transformed_data'

2. Run the ETL pipeline:

spark-submit spark_etl_pipeline.py

## License
This project is licensed under the MIT License. See the LICENSE file for details.

## Contribution Guidelines
Contributions are welcome! Please read the CONTRIBUTING.md file for guidelines on how to contribute to this project.

## Contact
If you have any questions or suggestions, feel free to open an issue or contact me at [j.a.carpenter778@gmail.com].

## Code of Conduct
Please note that this project is released with a Contributor Code of Conduct. By participating in this project, you agree to abide by its terms.

## Reporting Issues
If you encounter any issues, please open an issue in the repository with a clear description of the problem and any relevant details.
