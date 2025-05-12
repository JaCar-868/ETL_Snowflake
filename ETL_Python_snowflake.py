"""
ETL pipeline: extract CSV, transform data, and load into Snowflake.
Configuration via YAML file and environment variables.
"""

import os
import sys
import argparse
import logging
from typing import Iterator, Optional

import yaml
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas


def setup_logging(log_level: str = "INFO") -> None:
    """
    Configure structured logging.
    """
    logging.basicConfig(
        level=getattr(logging, log_level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )


def load_config(config_path: str) -> dict:
    """
    Load YAML configuration and override with environment variables if present.
    """
    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)

        sf_conf = config.get('snowflake', {}) or {}
        sf_conf['user'] = os.getenv('SNOWFLAKE_USER', sf_conf.get('user'))
        sf_conf['password'] = os.getenv('SNOWFLAKE_PASSWORD', sf_conf.get('password'))
        sf_conf['account'] = os.getenv('SNOWFLAKE_ACCOUNT', sf_conf.get('account'))
        config['snowflake'] = sf_conf
        return config

    except Exception as e:
        logging.error(f"Failed to load config file {config_path}: {e}")
        sys.exit(1)


def extract_csv(file_path: str, chunksize: Optional[int] = None) -> Iterator[pd.DataFrame]:
    """
    Read a CSV file and yield DataFrame or chunks thereof.
    """
    logging.info(f"Extracting CSV from {file_path} (chunksize={chunksize})")
    try:
        if chunksize:
            for chunk in pd.read_csv(file_path, chunksize=chunksize):
                yield chunk
        else:
            yield pd.read_csv(file_path)
    except FileNotFoundError:
        logging.error(f"CSV file not found: {file_path}")
        sys.exit(1)
    except Exception as e:
        logging.error(f"Error reading CSV file: {e}")
        sys.exit(1)


def transform_data(
    df: pd.DataFrame,
    drop_missing: bool = True,
    fill_missing_value: Optional[str] = None
) -> pd.DataFrame:
    """
    Transform DataFrame:
      - drop rows with missing values or
      - fill missing values with a specified constant.
    Additional cleansing rules can be added here.
    """
    logging.info(
        f"Transforming data: drop_missing={drop_missing}, fill_missing_value={fill_missing_value}"
    )
    if drop_missing:
        df = df.dropna()
    elif fill_missing_value is not None:
        df = df.fillna(fill_missing_value)

    return df


def load_to_snowflake(
    df: pd.DataFrame,
    conn_params: dict,
    table_name: str
) -> None:
    """
    Load DataFrame to Snowflake using write_pandas.
    """
    logging.info(f"Loading {len(df)} rows into Snowflake table {table_name}")
    try:
        with snowflake.connector.connect(
            user=conn_params['user'],
            password=conn_params['password'],
            account=conn_params['account'],
            warehouse=conn_params.get('warehouse'),
            database=conn_params.get('database'),
            schema=conn_params.get('schema')
        ) as conn:
            success, nchunks, nrows, _ = write_pandas(conn, df, table_name.upper())
            logging.info(f"write_pandas success={success}, chunks={nchunks}, rows={nrows}")
    except Exception as e:
        logging.error(f"Error loading to Snowflake: {e}")
        raise


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Run ETL pipeline: CSV to Snowflake"
    )
    parser.add_argument(
        '-c', '--config',
        type=str,
        default='config.yaml',
        help='Path to YAML configuration file.'
    )
    return parser.parse_args()


def main() -> None:
    """Main entry point for the ETL pipeline."""
    args = parse_args()
    setup_logging()
    config = load_config(args.config)

    csv_path = config.get('csv_path')
    table_name = config.get('table_name')
    conn_params = config['snowflake']
    chunk_size = config.get('chunk_size')

    if not csv_path or not table_name:
        logging.error("Both 'csv_path' and 'table_name' must be defined in the config file.")
        sys.exit(1)

    for df_chunk in extract_csv(csv_path, chunksize=chunk_size):
        transformed_df = transform_data(
            df_chunk,
            drop_missing=config.get('transform', {}).get('drop_missing', True),
            fill_missing_value=config.get('transform', {}).get('fill_missing_value')
        )

        if transformed_df.empty:
            logging.warning("Transformed DataFrame is empty; skipping load.")
            continue

        load_to_snowflake(transformed_df, conn_params, table_name)

    logging.info("ETL pipeline completed successfully.")


if __name__ == "__main__":
    main()
