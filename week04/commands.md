## Let's create an access to the DBT service on Google Cloud

### GCD-UI# Go to "APIs & Services" / "Credentials" / "Create Credentials" / "Help me choose"

--> Which API : BigQuery API
--> what data will you ...: application data
--> Are you planning to use ...: NO
--> NEXT

--> You can choose to reuse one of the service accounts you already have ... : CONTINUE

--> service account name: dtb-service-acount
--> service account id: dtb-service-acount
--> CREATE AND CONTINUE

--> role: "BigQuery Admin" OR "BigQuery Data Editor" + "BigQuery Job User" + "BigQuery User"
--> DONE

### GCD-UI# Once the account is created we can click on it, then go to "KEYS"
### GCD-UI# Go to "Add Key" / "Create new key" / "JSON" --> A json file is saved to the computer


## Create a DBT account
Got to https://www.getdbt.com/signup and create an account


## Setup a DBT project using BigQuery
See instructions here: https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/week_4_analytics_engineering/dbt_cloud_setup.md

*****
Optional: If you feel more comfortable developing locally you could use a local installation of dbt as well. You can follow the official dbt documentation or follow the dbt with BigQuery on Docker guide to setup dbt locally on docker. You will need to install the latest version (1.0) with the BigQuery adapter (dbt-bigquery).
*****

### Go to the DBT Cloud
--> Create Project & give it a name
--> Select "BigQuery" as datawarhouse

--> Upload the Google Cloud passkey.json generated earlier
--> Test Connection
--> NEXT

--> Setup Repository
--> Git Clone : git@github.com:Valkea/DE_bootcamp.git
--> IMPORTING
--> Copy the ssh-rsa KEY

--> GO to the github page of the project
--> "Settings" / "Deploy key" / "Add deploy key"
--> PASTE the ssh-rsa key 
--> Check the "Allow write access"
--> ADD KEY

--> Back to DBT project page
--> NEXT

--> Go to "Account settings" / "Projects" and click on the project name
--> EDIT
--> project subdirectory: week04
--> SAVE

--> Click "Develop"
--> Initialize project (it will add the init files in the week04 folder of the github)
--> Create a new branch ("DBT" for instance)

### Edit dbt_project.yml

---> name: taxi_rides_ny
---> models: taxi_rides_ny
---> models: remove examples


### (Alternative) DBT Core
the above project creation process can also be done locally using the terminal DBT cli and a profiles.yml file

--> fill profiles.yml
>>> dbt init


## Create DBT models

--> Go to models folder
--> Create "staging" and "core" sub-folder
--> Create file staging/schema.yml

> version: 2
> 
> sources:
>   - name: staging
>     database: lexical-passkey-375922
>     schema: homework_week4
> 
>     tables:
>       - name: green_tripdata
>       - name: yellow_tripdata
>       - name: fhv_tripdataversion: 1
> 
> sources:
>   - name: staging
>     database: lexical-passkey-375922
>     schema: homework_week4
> 
>     tables:
>       - name: green_tripdata
>       - name: yellow_tripdata
>       - name: fhv_tripdata

--> Create file staging/stg_green_tripdata.sql
(this is a simple baseline, see models/stg_green_tripdata.sql for complete file)

> {{ config(materialized='view') }}
> 
> select * from {{ source('staging', 'green_tripdata') }}
> limit 100

--> Use DBT cloud command line (bottom of IDE)

To run all models use:
>>> dbt run

To run a specific project use:
>>> dbt run --select stg_green_tripdata

************************************************************
## Transfer data to GCS bucket

```bash
>>> python web_to_gcs_gzip.py --color fhv --year 2019 --months 1 2 3 4 5 6 7 8 9 10 11 12

>>> python web_to_gcs_gzip.py --color green --year 2019 --months 1 2 3 4 5 6 7 8 9 10 11 12
>>> python web_to_gcs_gzip.py --color green --year 2020 --months 1 2 3 4 5 6 7 8 9 10 11 12

>>> python web_to_gcs_gzip.py --color yellow --year 2019 --months 1 2 3 4 5 6 7 8 9 10 11 12
>>> python web_to_gcs_gzip.py --color yellow --year 2020 --months 1 2 3 4 5 6 7 8 9 10 11 12
```

## Transfer data from GCS bucket to BigQuery

```bash
>>> python etl_gcs_to_bq.py -c fhv -y 2019 -m 1 2 3 4 5 6 7 8 9 10 11 12 -d homework_week4 -t fhv_tripdata

>>> python etl_gcs_to_bq.py -c green -y 2019 -m 1 2 3 4 5 6 7 8 9 10 11 12 -d homework_week4 -t green_tripdata
>>> python etl_gcs_to_bq.py -c green -y 2020 -m 1 2 3 4 5 6 7 8 9 10 11 12 -d homework_week4 -t green_tripdata

>>> python etl_gcs_to_bq.py -c yellow -y 2019 -m 1 2 3 4 5 6 7 8 9 10 11 12 -d homework_week4 -t yellow_tripdata
>>> python etl_gcs_to_bq.py -c yellow -y 2020 -m 1 2 3 4 5 6 7 8 9 10 11 12 -d homework_week4 -t yellow_tripdata
``` 

## Question 1:
> What is the count of records in the model fact_trips after running all models with the test run variable disabled and filtering for 2019 and 2020 data only (pickup datetime)?

```bash
>>> SELECT COUNT(1) FROM `lexical-passkey-375922.dbt_eletremble.fact_trips` WHERE pickup_datetime BETWEEN '2019-01-01 00:00:00' AND '2020-01-01 00:00:00';
```
#### The answer is : **46313976**

## Question 2: 
> What is the distribution between service type filtering by years 2019 and 2020 data as done in the videos . (Yellow/Green) *

#### According to the Google Data Studio dashbord, it's **89.9 / 10.1**

## Question 3:
> What is the count of records in the model stg_fhv_tripdata after running all models with the test run variable disabled

```bash
SELECT COUNT(1) FROM `lexical-passkey-375922.dbt_eletremble.stg_fhv_tripdata` WHERE true;
```

#### The answer is : **43244696**


## Setup for Q4 and Q5
In order to continue, we need to deploy the last changes to BQ

### Push the last version to our GitHub in order to deploy in production (if we want to use production, other wise, no need to deploy)

--> commit the DBT changes to the DBT branch on our Github
--> make a pull request
--> merge to main branch

### Deploy using the DBT-UI 

--> go to the "Deploy" / "Jobs" section and run the production job

### Create a dashboard using Google Data Studio 

--> check if the approriate table (act_fhv_trips) is in the BigQuery database

## Question 4:
> What is the count of records in the model fact_fhv_trips in 2019, after running all dependencies with the test run variable disabled 

```bash
SELECT COUNT(1) FROM `lexical-passkey-375922.dbt_eletremble.fact_fhv_trips` WHERE pickup_datetime BETWEEN '2019-01-01 00:00:00' AND '2020-01-01 00:00:00';
```

#### The answer is : **22998722**

## Question 5: 
> What is the month with the biggest amount of rides after building a tile for the fact_fhv_trips table
> Create a dashboard with some tiles that you find interesting to explore the data. One tile should show the amount of trips per month, as done in the videos for fact_trips, based on the fact_fhv_trips table.

--> see dashboard here: https://lookerstudio.google.com/reporting/5d04d288-5a15-41db-a8e3-f824ce8be571

#### The answer is : **January**
