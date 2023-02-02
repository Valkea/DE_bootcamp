import os
import time
import math
import argparse
from datetime import timedelta

import pandas as pd
# from sqlalchemy import create_engine

from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_sqlalchemy import SqlAlchemyConnector



@task(name="Extract Data", log_prints=True, retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def extract_data(url):
    if url.endswith('.csv.gz'):
        output_file = "output.csv.gz"
    else:
        output_file = "output.csv"

    os.system(f"wget {url} -O {output_file}")

    df = pd.read_csv(output_file)
    return df


@task(name="Transform Data", log_prints=True)
def transform_data(df):

    print(f"pre: columns types: \n{df.dtypes}")
    df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
    print(f"post: columns types: \n{df.dtypes}")

    print(f"pre: missing passenger count: {df[df.passenger_count == 0].shape[0]} / {df.shape[0]}")
    df = df[df.passenger_count != 0]
    print(f"post: missing passenger count: {df[df.passenger_count == 0].shape[0]} / {df.shape[0]}")
    return df


@task(name="Load Data into DB", log_prints=True, retries=3)
def ingest(df, table):

    # engine = create_engine(f'postgresql://{args.user}:{args.password}@{args.host}:{args.port}/{args.db}')
    database_block = SqlAlchemyConnector.load("ny-taxi-postgres-connector")
    with database_block.get_connection(begin=False) as engine:

        print("Create the table")
        df.head(n=0).to_sql(name=table, con=engine, if_exists='replace')

        # Fill the table with the rows
        print("Start inserting data")

        step_size = 100000
        for i in range(0, math.ceil(df.shape[0] / step_size)):

            start = i * step_size
            end = min((i + 1) * step_size, df.shape[0])
            t0 = time.time()

            chunk = df.iloc[start: end]
            chunk.to_sql(name=table, con=engine, if_exists='append')

            print(f"Inserted another chunk ({chunk.shape[0]})... took {time.time()-t0} secondes")


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
    # args.user = 'root'
    # args.password = 'root'
    # args.host = 'localhost'
    # args.port = '5432'
    # args.db = 'ny_taxi'
    args.table = 'green_taxi_data'
    args.csv_url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz'
    # / TMP ?

    raw_data = extract_data(args.csv_url)
    trans_data = transform_data(raw_data)
    ingest(trans_data, args.table)


if __name__ == "__main__":
    main_flow()
