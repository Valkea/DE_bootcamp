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

--> The project is ready

### DBT Core
the above project creation process can also be done locally using the terminal DBT cli and a profiles.yml file

--> fill profiles.yml
>>> dbt init

************************************************************
## Transfer data to GCS bucket

```bash
>>> python web_to_gcs_gzip.py --color fhv --year 2019 --months 1 2 3 4 5 6 7 8 9 10 11 12

>>> python web_to_gcs_gzip.py --color green --year 2019 --months 1 2 3 4 5 6 7 8 9 10 11 12
>>> python web_to_gcs_gzip.py --color green --year 2020 --months 1 2 3 4 5 6 7 8 9 10 11 12

>>> python web_to_gcs_gzip.py --color yellow --year 2019 --months 1 2 3 4 5 6 7 8 9 10 11 12
>>> python web_to_gcs_gzip.py --color yellow --year 2020 --months 1 2 3 4 5 6 7 8 9 10 11 12
```
