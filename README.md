## ETL_Snowflake

This repository provides a configurable, production-ready ETL pipeline in Python. It extracts data from a CSV file, performs flexible transformations, and loads it into a Snowflake data warehouse with structured logging and a CLI interface.

## Features

Chunked extraction for large CSV files via pandas.read_csv(chunksize=…)

Flexible transformations: drop or fill missing values

Secure Snowflake connection through environment variables or a YAML config file

Structured logging using Python’s logging module

Command-line interface with argparse and an __main__ guard

Configurable via a config.yaml file

## Prerequisites

Python 3.6 or higher

Snowflake account with appropriate privileges

## Python libraries:

pandas

PyYAML

snowflake-connector-python

## Installation

Clone the repository

git clone https://github.com/JaCar-868/ETL_Snowflake.git
cd ETL_Snowflake

(Optional) Create and activate a virtual environment:

python3 -m venv venv
source venv/bin/activate

## Install dependencies

pip install -r requirements.txt

## Configuration

Create a config.yaml file at the project root. Example:

csv_path: data/input.csv       # Path to your CSV

# Target Snowflake table name
table_name: MY_TABLE

# Optional: number of rows per chunk (omit or set null to read all at once)
chunk_size: 5000

transform:
  # Drop rows containing any missing values
  drop_missing: true
  # Or fill missing values with a constant (e.g. "UNKNOWN")
  fill_missing_value: null

snowflake:
  user: YOUR_USER              # or set SNOWFLAKE_USER env var
  password: YOUR_PASSWORD      # or set SNOWFLAKE_PASSWORD env var
  account: YOUR_ACCOUNT        # or set SNOWFLAKE_ACCOUNT env var
  warehouse: YOUR_WAREHOUSE
  database: YOUR_DATABASE
  schema: YOUR_SCHEMA

Tip: Any user, password, or account fields in config.yaml will be overridden by the corresponding environment variables.

## Usage

Run the pipeline with:

python ETL_Python_snowflake.py --config config.yaml

View help and available flags:

python ETL_Python_snowflake.py -h

## Logging

By default, logs are printed to the console at the INFO level. To change verbosity, set the LOG_LEVEL environment variable:

export LOG_LEVEL=DEBUG

## Testing

Unit tests are written with pytest. To run all tests:

pytest tests/

## Contributing

Contributions are welcome! Please read CONTRIBUTING.md for guidelines.

## License
This project is licensed under the MIT License. See the [LICENSE](https://github.com/JaCar-868/Disease-Progression/blob/main/LICENSE) file for details.

## Contribution Guidelines
Contributions are welcome! Please read the CONTRIBUTING.md file for guidelines on how to contribute to this project.

## Contact
If you have any questions or suggestions, feel free to open an issue or contact me at [j.a.carpenter778@gmail.com].

## Code of Conduct
Please note that this project is released with a Contributor Code of Conduct. By participating in this project, you agree to abide by its terms.

## Reporting Issues
If you encounter any issues, please open an issue in the repository with a clear description of the problem and any relevant details.
