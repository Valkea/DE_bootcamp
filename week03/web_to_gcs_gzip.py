#! /usr/bin/env python3

import os
import pathlib
import urllib
import argparse
from datetime import timedelta

from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect.tasks import task_input_hash

@task(
    name="Extract Data",
    log_prints=True,
    retries=3,
    # cache_key_fn=task_input_hash,
    # cache_expiration=timedelta(days=1),
)
def fetch(dataset_url: str, dataset_file: str) -> pathlib.Path:
    """Download data from web"""

    print("FETCH", dataset_url)
    in_path = pathlib.Path("tmp")
    if not in_path.exists():
        os.makedirs(in_path)

    tmp_path = pathlib.Path(in_path, dataset_file)

    urllib.request.urlretrieve(dataset_url, tmp_path)

    return in_path

@task(name="Write Data on GCS", log_prints=True)
def write_GCS(in_path: pathlib.Path, out_path: pathlib.Path, filename: str) -> None:
    """Copy the file to GCS"""

    in_path_file = pathlib.Path(in_path, filename)
    out_path_file = pathlib.Path(out_path, filename)

    gcs_block = GcsBucket.load("ny-taxi-gcs-bucket")
    gcs_block.upload_from_path(from_path=in_path_file, to_path=out_path_file)


@flow(log_prints=True, retries=1)
def web_to_gcs(year: int, month: int, color: str) -> None:
    """The main Transfer function"""

    dataset_file = f"{color}_tripdata_{year}-{month:02}.csv.gz"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}"

    in_path = fetch(dataset_url, dataset_file)
    out_path = pathlib.Path('homework_w03')

    write_GCS(in_path, out_path, dataset_file)


@flow(log_prints=True)
def main(
    year: int = 2020, months: list = [1, 2], color: str = "yellow"
) -> None:
    """The main function"""
    for month in months:
        print(f"Transfer data from WEB to GCS for year:{year} month:{month} color:{color}")
        web_to_gcs(int(year), int(month), color)


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

    main(year, months, color)
