import os
import sys
import time
import argparse

import pandas as pd
from sqlalchemy import create_engine
from prefect import flow, task


@task(name="Ingest function", log_prints=True, retries=3)
def ingest(args):

    if args.csv_url.endswith('.csv.gz'):
        output_file = "output.csv.gz"
    else:
        output_file = "output.csv"

    os.system(f"wget {args.csv_url} -O {output_file}")

    engine = create_engine(f'postgresql://{args.user}:{args.password}@{args.host}:{args.port}/{args.db}')

    # Create table with columns
    print(f"Create / Replace table {args.table} in DB {args.db}")

    dt = pd.read_csv(output_file)
    dt.lpep_pickup_datetime = pd.to_datetime(dt.lpep_pickup_datetime)
    dt.lpep_dropoff_datetime = pd.to_datetime(dt.lpep_dropoff_datetime)

    dt.head(n=0).to_sql(name=args.table, con=engine, if_exists='replace')

    # Fill the table with the rows
    print("Start inseting data")

    df_iter = pd.read_csv(output_file, iterator=True, chunksize=100000)

    for chunk in df_iter:

        t0 = time.time()

        chunk.lpep_pickup_datetime = pd.to_datetime(chunk.lpep_pickup_datetime)
        chunk.lpep_dropoff_datetime = pd.to_datetime(chunk.lpep_dropoff_datetime)
        chunk.to_sql(name=args.table, con=engine, if_exists='append')

        print(f"inserted another chunk ({len(chunk)})... took {time.time()-t0} secondes")


@flow(name='Ingest Flow')
def main_flow():

    parser = argparse.ArgumentParser()
    # parser.add_argument("--user", help="user name for postgres")
    # parser.add_argument("--password", help="password for postgres")
    # parser.add_argument("--host", help="host for postgres")
    # parser.add_argument("--port", help="port for postgres")
    # parser.add_argument("--db", help="database name for postgres")
    # parser.add_argument("--table", help="the name of the table to fill with the CSV")
    # parser.add_argument("--csv_url", help="the url to the csv file")

    args = parser.parse_args()

    # TMP ?
    args.user = 'root'
    args.password = 'root'
    args.host = 'localhost'
    args.port = '5432'
    args.db = 'ny_taxi'
    args.table = 'green_taxi_data'
    args.csv_url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz'
    # / TMP ?

    ingest(args)


if __name__ == "__main__":
    main_flow()
