#! /usr/bin/env python3

import os
import pathlib
import pandas as pd
import argparse
from datetime import timedelta

from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials
from prefect.tasks import task_input_hash


@task(retries=3)
def extract_from_gcs(src_path: pathlib.Path, filename: str) -> pathlib.Path:
    """Download trip data from GCS"""

    tmp_path = pathlib.Path("data_tmp")
    if not tmp_path.exists():
        os.makedirs(tmp_path)

    tmp_path_file = pathlib.Path(tmp_path, filename)

    gcs_path = pathlib.Path(src_path, filename)

    gcs_block = GcsBucket.load("ny-taxi-gcs-bucket")
    gcs_block.get_directory(from_path=gcs_path, local_path=tmp_path_file)

    return tmp_path_file


@task(name="Transform Data", log_prints=True)
def transform_data(path: pathlib.Path) -> pd.DataFrame:
    """Data cleaning example"""

    print(f"Read parquet file in TMP directory: {path}")
    df = pd.read_parquet(path)

    # print(
    #     f"pre: missing passenger count: {df.passenger_count.isna().sum()} / {df.shape[0]}"
    # )
    # df.passenger_count.fillna(0, inplace=True)
    # print(
    #     f"post: missing passenger count: {df.passenger_count.isna().sum()} / {df.shape[0]}"
    # )

    print(f"row count: {df.shape[0]}")

    return df


@task(retries=3)
def write_bq(df: pd.DataFrame) -> None:
    """Write the DataFrame to Big Query Data Warehouse"""

    gcp_credentials_block = GcpCredentials.load("ny-taxi-gcs-creds")
    df.to_gbq(
        destination_table="homework_week2.taxi-rides",
        project_id="lexical-passkey-375922",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500000,
        if_exists="append",
    )


@flow(name="", log_prints=True)
def etl_gcs_to_bq(year: int, month: int, color: str) -> None:
    """Main ETL flow to load data into Big Query Data Warehouse"""

    filename = f"{color}_tripdata_{year}-{month:02}.parquet"
    src_path = pathlib.Path("homework")

    tmp_path = extract_from_gcs(src_path, filename)
    df = transform_data(tmp_path)
    write_bq(df)


@flow(log_prints=True)
def etl_parent_flow(year: int, months: list, color: str) -> None:
    """The main function"""
    for month in months:
        print(f"ETL for year:{year} month:{month} color:{color}")
        etl_gcs_to_bq(int(year), int(month), color)


if __name__ == "__main__":

    year = 2020
    months = [1, 2, 3]
    color = "green"

    parser = argparse.ArgumentParser()
    parser.add_argument("--color", help="the color of the dataset (default=green)")
    parser.add_argument("--year", help="the year of the dataset (default=2021)")
    parser.add_argument(
        "--months",
        nargs="+",
        help="the LIST of month to grab in the given year (default=1 2 3)",
    )
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
