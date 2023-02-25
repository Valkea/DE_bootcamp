#!/usr/bin/env python
# coding: utf-8

import argparse

import pyspark
from pyspark.sql import functions as F
from pyspark.sql import SparkSession


def main(input_green:str, input_yellow:str, output:str) -> None :
    
    #### Let's connect the local Spark cluster
    print("==> Connect local Spark Cluster")
    
    # We will use spark-submit instead of the master, so we emove the master argument
    spark = SparkSession.builder \
        .appName('test') \
        .getOrCreate()

    #### Let's load the datasets
    print("==> Load datasets")
    
    df_green = spark.read.parquet(input_green) # data/pq/green/all_years/all_months
    df_yellow = spark.read.parquet(input_yellow) # data/pq/yellow/all_years/all_months

    #### Let's rename the dropoff and pickup columns
    print("==> Transforming data")

    df_green = df_green \
        .withColumnRenamed('lpep_pickup_datetime', 'pickup_datetime') \
        .withColumnRenamed('lpep_dropoff_datetime', 'dropoff_datetime')

    df_yellow = df_yellow \
        .withColumnRenamed('tpep_pickup_datetime', 'pickup_datetime') \
        .withColumnRenamed('tpep_dropoff_datetime', 'dropoff_datetime')

    #### Let's find common columns

    col_names = []

    commun_cols = set(df_green.columns) & set(df_yellow.columns)
    for col in df_green.columns:
        if col in commun_cols:
            col_names.append(col)

    #### Let's add a new column with the original dataset name

    df_green_plus = df_green \
        .select(col_names) \
        .withColumn('service_type', F.lit("green"))

    df_yellow_plus = df_yellow \
        .select(col_names) \
        .withColumn('service_type', F.lit("yellow"))

    #### Let's combine in order to get `trips_data`
    print("==> Combine datasets")

    df_trips_data = df_green_plus.unionAll(df_yellow_plus)
    # df_trips_data.groupBy('service_type').count().show()


    #### Let's make the SQL requests
    print("==> Make SQL request")

    df_trips_data.createOrReplaceTempView('trips_data')

    df_result = spark.sql("""
        SELECT 
        -- Revenue grouping 
        PULocationID AS revenue_zone,
        date_trunc('month', pickup_datetime) AS revenue_month, 
        service_type, 

        -- Revenue calculation 
        SUM(fare_amount) AS revenue_monthly_fare,
        SUM(extra) AS revenue_monthly_extra,
        SUM(mta_tax) AS revenue_monthly_mta_tax,
        SUM(tip_amount) AS revenue_monthly_tip_amount,
        SUM(tolls_amount) AS revenue_monthly_tolls_amount,
        -- SUM(ehail_fee) AS revenue_monthly_ehail_fee,
        SUM(improvement_surcharge) AS revenue_monthly_improvement_surcharge,
        SUM(total_amount) AS revenue_monthly_total_amount,
        SUM(congestion_surcharge) AS revenue_monthly_congestion_surcharge,

        -- Additional calculations
        -- count(tripid) AS total_monthly_trips,
        AVG(passenger_count) AS avg_montly_passenger_count,
        AVG(trip_distance) AS avg_montly_trip_distance

        FROM trips_data
        GROUP BY 1,2,3
    """)

    #### Let's push data back to the data-lake (we pretend it is)

    print("==> Write parquet files")
    df_result.coalesce(1).write.parquet(output, mode='overwrite') # df_result.coalesce(1) reduce partitions to 1
          
    print("==> Done")

if __name__ == "__main__":
    
    parser = argparse.ArgumentParser()
    parser.add_argument('-g', '--input_green', required=True)
    parser.add_argument('-y', '--input_yellow', required=True)
    parser.add_argument('-o', '--output', required=True)
    
    args = parser.parse_args()
    
    main(args.input_green, args.input_yellow, args.output)