#! /usr/bin/env python3

import pathlib
import pandas as pd
from datetime import timedelta

from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials
from prefect.tasks import task_input_hash


@task(retries=3)
def extract_from_gcs(dataset_file: str) -> pathlib.Path:
    """ Download trip data from GCS """

    gcs_path = pathlib.Path("data", dataset_file)
    localbase = "data2"
    localpath = pathlib.Path(localbase, gcs_path)

    gcs_block = GcsBucket.load("ny-taxi-gcs-bucket")
    gcs_block.get_directory(from_path=gcs_path, local_path=localbase)

    return localpath


@task(name="Transform Data", log_prints=True)
def transform_data(path: pathlib.Path) -> pd.DataFrame:
    """ Data cleaning example """

    df = pd.read_parquet(path)

    print(
        f"pre: missing passenger count: {df.passenger_count.isna().sum()} / {df.shape[0]}"
    )
    df.passenger_count.fillna(0, inplace=True)
    print(
        f"post: missing passenger count: {df.passenger_count.isna().sum()} / {df.shape[0]}"
    )

    return df


@task(retries=3)
def write_bq(df: pd.DataFrame) -> None:
    """ Write the DataFrame to Big Query Data Warehouse """

    gcp_credentials_block = GcpCredentials.load("ny-taxi-gcs-creds")
    df.to_gbq(
        destination_table="dezoomcamp_dataset_us.taxi-rides",
        project_id="lexical-passkey-375922",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500000,
        if_exists="append",
    )


@flow(name="", log_prints=True)
def etl_gcs_to_bq():
    """ Main ETL flow to load data into Big Query Data Warehouse """

    print("ETL_GCS_TO_BQ")
    color = "green"
    year = 2020
    month = 1

    dataset_file = f"{color}_tripdata_{year}-{month:02}.parquet"

    path = extract_from_gcs(dataset_file)
    df = transform_data(path)
    write_bq(df)


if __name__ == "__main__":
    print("INIT")
    etl_gcs_to_bq()
