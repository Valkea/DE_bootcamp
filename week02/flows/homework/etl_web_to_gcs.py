#! /usr/bin/env python3

import os
import pathlib
import pandas as pd
import argparse
from datetime import timedelta

from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect.tasks import task_input_hash


@task(
    name="Extract Data",
    log_prints=True,
    retries=3,
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(days=1),
)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read data from web into pandas DataFrame"""

    print("FETCH", dataset_url)
    return pd.read_csv(dataset_url)


@task(name="Transform Data", log_prints=True)
def transform_data(df: pd.DataFrame) -> pd.DataFrame:

    print(df.head(2))

    print(f"pre: columns types: \n{df.dtypes}")
    if "lpep_pickup_datetime" in df.columns:
        df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
        df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
    elif "tpep_pickup_datetime" in df.columns:
        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    print(f"post: columns types: \n{df.dtypes}")

    # Answer Q1: find out how many rows the dataset has.
    print(f"row count: {df.shape[0]}")

    # print(f"pre: missing passenger count: {df[df.passenger_count == 0].shape[0]} / {df.shape[0]}")
    # df = df[df.passenger_count != 0]
    # print(f"post: missing passenger count: {df[df.passenger_count == 0].shape[0]} / {df.shape[0]}")

    return df


@task(name="Write Data Locally", log_prints=True)
def write_local(df: pd.DataFrame, dataset_file: str) -> pathlib.Path:
    """Write the DataFrame out locally as a Parquet file"""

    in_path = pathlib.Path("data")
    if not in_path.exists():
        os.makedirs(in_path)

    filename = f"{dataset_file.replace('.csv.gz','.parquet')}"

    df.to_parquet(pathlib.Path(in_path, filename), compression="gzip")

    return in_path, filename


@task(name="Write Data on GCS", log_prints=True)
def write_GCS(in_path: pathlib.Path, out_path: pathlib.Path, filename: str) -> None:
    """Copy the Parquet file to GCS"""

    in_path_file = pathlib.Path(in_path, filename)
    out_path_file = pathlib.Path(out_path, filename)

    gcs_block = GcsBucket.load("ny-taxi-gcs-bucket")
    gcs_block.upload_from_path(from_path=in_path_file, to_path=out_path_file)


@flow(log_prints=True, retries=3)
def etl_web_to_gcs(year: int, month: int, color: str) -> None:
    """The main ETL function"""

    dataset_file = f"{color}_tripdata_{year}-{month:02}.csv.gz"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}"

    df = fetch(dataset_url)
    df_clean = transform_data(df)
    in_path, filename = write_local(df_clean, dataset_file)
    out_path = pathlib.Path('homework')
    write_GCS(in_path, out_path, filename)


@flow(log_prints=True)
def etl_parent_flow(
    year: int = 2020, months: list = [1, 2], color: str = "yellow"
) -> None:
    """The main function"""
    for month in months:
        print(f"ETL for year:{year} month:{month} color:{color}")
        etl_web_to_gcs(int(year), int(month), color)


if __name__ == "__main__":

    year = 2020
    months = [1, 2, 3]
    color = "green"

    parser = argparse.ArgumentParser()
    parser.add_argument("--color", help="the color of the dataset (default=green)")
    parser.add_argument("--year", help="the year of the dataset (default=2021)")
    parser.add_argument("--months", nargs='+', help="the LIST of month to grab in the given year (default=1 2 3)")
    args = parser.parse_args()

    if args.color:
        color = args.color
    if args.year:
        year = args.year
    if args.months:
        try:
            months = list(args.months)
        except Exception as e:
            raise e

    etl_parent_flow(year, months, color)
