

## Let's transfer the GZ files to the GCS Bucket
For this we will use a modfied version of the script wrote in Week 1 & 2

>>> prefect orion start
>>> python web_to_gcs_gzip.py --color fhv --year 2019 --months 1 2 3 4 5 6 7 8 9 10 11 12

## Let's setup the BigQuery database using the downloaded GZ files

### Create a new database
> Big-Query-GUI ==> Right click on 'lexical-passkey-375922' and 'create new database'
> ---> name: jomework_week3

### CREATE AN EXTERNAL TABLE USING THE fhv 2019 DATA 

```bash
CREATE OR REPLACE EXTERNAL TABLE `lexical-passkey-375922.homework_week3.taxi-rides_texternal`
OPTIONS (
  format = 'CSV',
  uris = ['gs://week02_02_gcp_bucket/homework_w03/fhv_tripdata_2019-*.csv.gz']
);
```

### CREATE A TABLE IN BQ USING THE FHV 2019 DATA (DO NOT PARTITION OR CLUSTER THIS TABLE). 

```bash
CREATE OR REPLACE TABLE `lexical-passkey-375922.homework_week3.taxi-rides_internal` AS SELECT * FROM `lexical-passkey-375922.homework_week3.taxi-rides_external`;
```


## Answer Q1:
> *What is the count for fhv vehicle records for year 2019?*

```bash
SELECT COUNT(1) FROM `lexical-passkey-375922.homework_week3.taxi-rides_external` WHERE true;
```

>> The answer is : **43244696**


## Answer Q2:
> *Write a query to count the distinct number of affiliated_base_number for the entire dataset on both the tables*.
>
> *What is the estimated amount of data that will be read when this query is executed on the External Table and the BQ Table?*

```bash
SELECT DISTINCT Affiliated_base_number FROM `lexical-passkey-375922.homework_week3.taxi-rides_external` WHERE true;
```

> This query will process **0 B** when run.

```bash
SELECT DISTINCT Affiliated_base_number FROM `lexical-passkey-375922.homework_week3.taxi-rides_internal` WHERE true;
```

>> This query will process **317.94 MB** when run.


## Answer Q3:
> *How many records have both a blank (null) PUlocationID and DOlocationID in the entire dataset?*

```bash
SELECT COUNT(1) FROM `lexical-passkey-375922.homework_week3.taxi-rides_external` WHERE PUlocationID IS NULL AND DOlocationID IS NULL;
```

>> The answer is : **717748**


## Answer Q4:
> *What is the best strategy to optimize the table if query always filter by pickup_datetime and order by affiliated_base_number?*

>> **Partition by pickup_datetime Cluster on affiliated_base_number** most probably

```bash
CREATE OR REPLACE TABLE `lexical-passkey-375922.homework_week3.taxi-rides_partitioned`
PARTITION BY DATE(pickup_datetime)
CLUSTER BY Affiliated_base_number
AS SELECT * FROM `lexical-passkey-375922.homework_week3.taxi-rides_internal`;
```


## Answer Q5:  
> *Write a query to retrieve the distinct affiliated_base_number between pickup_datetime 03/01/2019 and 03/31/2019 (inclusive). Use the materialized table you created earlier in your from clause and note the estimated bytes.*

```bash
SELECT DISTINCT Affiliated_base_number FROM `lexical-passkey-375922.homework_week3.taxi-rides_internal` 
WHERE pickup_datetime > PARSE_TIMESTAMP("%m/%d/%Y", "03/01/2019") 
AND pickup_datetime <= PARSE_TIMESTAMP("%m/%d/%Y", "03/31/2019")
```

>> This query will process **647.87 MB** when run.

> *Now change the table in the from clause to the partitioned table you created for question 4 and note the estimated bytes processed. What are these values?*

```bash
SELECT DISTINCT Affiliated_base_number FROM `lexical-passkey-375922.homework_week3.taxi-rides_partitioned` 
WHERE pickup_datetime > PARSE_TIMESTAMP("%m/%d/%Y", "03/01/2019") 
AND pickup_datetime <= PARSE_TIMESTAMP("%m/%d/%Y", "03/31/2019")
```
>> This query will process **23.05 MB** when run.


## Answer Q6
> *Where is the data stored in the External Table you created?*

>> The answer is **GCP Bucket**


## Answer Q7
> *It is best practice in Big Query to always cluster your data?*

>> The answer is **False** because sometimes the tables are small and in this case it's not recommanded to use PARTITION and CLUSTER (otherwise yes)