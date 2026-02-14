# -*- coding: utf-8 -*-
"""
Data preprocessing module for NYC Yellow Taxi trip data.

This module provides functionality to:
- Load raw taxi trip data from S3/MinIO storage
- Clean and transform the raw data
- Prepare features and target for machine learning
- Save processed data in multiple formats

Typical usage example:
    from data_preprocessing import get_x_y
    X, y = get_x_y()
"""

import pandas as pd
import dask.dataframe as dd
import s3fs
from typing import List

# Configuration constants
DATA_PATH = "s3://raw/yellow_tripdata/year=2025/"
"""str: Base path for raw data in S3/MinIO storage."""

SAVE_PATH = "../data/processed/"
"""str: Local path where processed data will be saved."""

MONTHS = [
    "month=01",
    # "month=02",  # Commented months for incremental processing
    # "month=03",
    # "month=04",
    # "month=05",
    # "month=06",
    # "month=07",
    # "month=08",
    # "month=09",
    # "month=10",
    # "month=11",
    # "month=12/"  # Not added for 2025 as data might be incomplete
]
"""List[str]: List of months to process (currently only January 2025)."""

def load_yearly_raw_data() -> dd.DataFrame:
    """
    Load raw taxi trip data from S3/MinIO storage using Dask for lazy evaluation.

    Returns:
        dd.DataFrame: Dask DataFrame containing all raw data for the specified months

    Note:
        - Uses MinIO as S3-compatible storage
        - Performs lazy loading for memory efficiency
        - Only loads parquet files matching the pattern
    """
    storage_options = {
        "key": "minio",  # MinIO access key
        "secret": "minio123",  # MinIO secret key
        "client_kwargs": {"endpoint_url": "http://localhost:9000"}  # MinIO server URL
    }
    fs = s3fs.S3FileSystem(**storage_options)

    # Find all parquet files for the specified months
    months = []
    for month in MONTHS:
        months.extend(["s3://" + f for f in fs.glob(f"s3a://nyc-raw/yellow_tripdata/year=2025/{month}/part-*.parquet")])

    # Lazy loading with Dask (data isn't loaded into memory yet)
    raw_data = dd.read_parquet(
        months,
        storage_options=storage_options,
        engine="pyarrow"  # Using pyarrow engine for better performance
    )
    return raw_data

def hhmmss_to_timedelta(x: int) -> pd.Timedelta:
    """
    Convert HHMMSS format (as integer) to pandas Timedelta.

    Args:
        x (int): Time in HHMMSS format (e.g., 123456 for 12:34:56)

    Returns:
        pd.Timedelta: Equivalent timedelta object

    Example:
        >>> hhmmss_to_timedelta(123456)
        Timedelta('0 days 12:34:56')
    """
    x = f"{int(x):06d}"  # Ensure 6-digit format with leading zeros
    h = int(x[:2])
    m = int(x[2:4])
    s = int(x[4:6])
    return pd.Timedelta(hours=h, minutes=m, seconds=s)

def clean_raw_data(raw_data: dd.DataFrame) -> pd.DataFrame:
    """
    Clean and transform raw taxi trip data for machine learning.

    Current transformations:
    - Selects only relevant features
    - Drops unused datetime columns
    - Saves processed data in parquet and CSV formats

    Args:
        raw_data (dd.DataFrame): Raw data loaded from storage

    Returns:
        pd.DataFrame: Cleaned and processed data as pandas DataFrame

    Note:
        - Several datetime processing features are currently commented out
        - Uses Dask for memory-efficient processing of large datasets
    """
    # Select only the features we need for our ML model
    sorted_data = raw_data[get_features()]

    # TODO: Uncomment and fix datetime processing when needed
    # Currently commented out as it appears to have bugs:
    # 1. Using dropoff_datetime for both pickup and dropoff
    # 2. Duration calculation is commented out

    # Cleaning unused columns
    clean_data = sorted_data.drop(
        columns=["tpep_pickup_datetime", "tpep_dropoff_datetime"]
        # , "pu_hhmmss", "do_hhmmss"]  # These would be created by datetime processing
    )

    # Compute the Dask DataFrame to pandas (triggers actual computation)
    clean_data_pd = clean_data.compute()

    # Save processed data in multiple formats
    clean_data_pd.to_parquet(
        path=(SAVE_PATH + "clean_data.parquet"),
        compression="snappy"  # Good balance between speed and compression
    )
    clean_data_pd.to_csv(path_or_buf=(SAVE_PATH + "clean_data.csv"))

    return clean_data_pd

def get_features() -> List[str]:
    """
    Get the list of features used for machine learning.

    Returns:
        List[str]: List of feature names
    """
    return [
        "PULocationID",         # Pickup location ID
        "DOLocationID",         # Dropoff location ID
        "tpep_pickup_datetime", # Pickup datetime (currently dropped)
        "tpep_dropoff_datetime",# Dropoff datetime (currently dropped)
        "passenger_count",      # Number of passengers
        "fare_amount",          # Base fare amount
        "extra",                # Extra charges
        "tip_amount",           # Tip amount
        "trip_distance",        # Trip distance in miles
        "total_amount"          # Total amount (target variable)
    ]

def get_x_y() -> tuple[pd.DataFrame, pd.Series]:
    """
    Main function that orchestrates the data loading and preprocessing pipeline.

    Returns:
        tuple: (X, y) where:
            - X (pd.DataFrame): Features for machine learning
            - y (pd.Series): Target variable (total_amount)

    Note:
        This is the main entry point for the data preprocessing module
    """
    raw_data = load_yearly_raw_data()
    clean_data = clean_raw_data(raw_data=raw_data)

    # Separate features and target
    X = clean_data.drop(columns=["total_amount"])
    y = clean_data["total_amount"]

    return X, y
