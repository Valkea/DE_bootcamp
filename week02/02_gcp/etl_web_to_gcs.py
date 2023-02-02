#! /usr/bin/env python3

import pathlib
import pandas as pd

from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket


@task(log_prints=True, retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """ Read data from web into pandas DataFrame """

    print("FETCH", dataset_url)
    return pd.read_csv(dataset_url)

@flow(log_prints=True, retries=3)
def etl_web_to_gcs() -> None:
    """ The main ETL function """

    print("ETL_WEB_TO_GCS")
    color = "green"
    year = 2021
    month = 1
    dataset_file = f"{color}_tripdata_{year}-{month:02}.csv.gz"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}"

    df = fetch(dataset_url)


if __name__ == "__main__":
    print("INIT")
    etl_web_to_gcs()
