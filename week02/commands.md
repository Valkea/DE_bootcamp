## Install PREFECT and other libs

>>> python -m venv venvWeek01

>>> pip install -r requirements.txt

>>> prefect version



## Edit ingest_data.py

### import flow and task from prefect
### create a flow function to organize the tasks (let's use the main function)
### create a task function (let's convert the ingest function) 

### Split the original function into ETL (Extract, Transform, Load) steps



## Run local PREFECT GUI

>>> prefect orion start

### open http://127.0.0.1:4200/



# Use PREFECT compatible libs (pre-configured to run with PREFECT)
https://docs.prefect.io/collections/catalog/

## 1. Let's externalize the SQLALCHEMY parameters from our script

>>> pip install prefect-sqlalchemy

### PREFECT-GUI ## "Blocks" / "Add Block" / "SQLAlchemy Connector"

name --> ny-taxi-postgres-connector
driver --> "SyncDriver" / "postgresql + psychopg2"

database --> ny_taxi
username --> root
password --> root
host --> localhost
port --> 5432

### Edit script
==> Remove Postgres parameters and create_engine call
==> Add SqlAlchemyConnector instead (as explained on the PREFECT-GUI SqLAlchemy block page)


# --------------------------------------------------------------
# Do ETL and then send the resulting file to Google Cloud Storage (gcs)

we write a whole new script (see 02_gcp)

## GCP-UI ## "Cloud Storage" / "Buckets" / "New"

name --> week02_02_gcp_bucket

Click on "CREATE"

## GCP-UI ## "Cloud Storage" / "Buckets" / "week02_02_gcp_bucket"

Here you can see the content of the bucket

# (Optional) GCD-UI# Go to "IAM & Admin" / "Service Account"
# (Optional) GCD-UI# Go to "Create service account"

-->1/ service account-name: PROJECT-NAME-user-bucket (dt-dt-w2-user-bucket)
-->2/ role: "BigQuery Admin" & "Cloud Storage / Storage Admin"
--> Save
-->3/ .
--> Done

#GCD-UI# Once the service account is created we can click '...' and "Manage keys"
#GCD-UI# Go to "Add Key" / "Create new key" / "JSON" --> A json file is saved to the computer



## Let's add the PREFECT GCP Block

Register blocks types within a module or file.                                                                                                                                                                                   
### Make sure the targeted blocks is available for configuration via the UI. (If a block type has already been registered, its registration will be updated to match the block's current definition)

>>> prefect block register -m prefect_gcp 

### PREFECT-GUI ## "Blocks" / "Add Block" / "GCS Bucket"

name --> ny-taxi-gcs-bucket
bucket-name --> week02_02_gcp_bucket

### Click "ADD" on GCP-Credentials

name --> ny-taxi-gcs-creds
service account info --> copy the content of the JSON file associated with the bucket account (we downloaded it earlier)

--> Done
--> Select the newly created  GCP credential in the select box
--> Create


### Edit script
==> Add GcsBucket (as explained on the PREFECT-GUI GcSBucket block page)

# --------------------------------------------------------------
# Move the GCS files to BigQuery Data Warehouse

we write a whole new script (see etl_gcs_to_bq.py in 02_gcp)

## GCP-UI ## "Big Query" / "Add data" / "Google Cloud Storage"

--> Browse to "week02_02_gcp_bucket/data/green_tripdata_2020-01.parquet"
--> Project : lexical-passkey-375922
--> Dataset : create a new one with "dezoomcamp_dataset_us" and select it
--> Table : taxi-rides
--> Click CREATE TABLE

## Edit script
==> Add df.to_gbd (google big query) call
==> Use the GcpCredentials defined earlier (go to GcpCredentials block on Orion and copy code there)


> ### WHAT is **BIG QUERY**?
> BigQuery is a fully managed enterprise **data warehouse** that helps you manage and analyze your data with **built-in features** like *machine learning, geospatial analysis, and business intelligence*.
> 
> BigQuery's **serverless architecture** lets you use **SQL queries** to answer your organization's biggest questions with zero infrastructure management.
> 
> BigQuery's scalable, distributed analysis engine **lets you query terabytes in seconds and petabytes in minutes**.



# --------------------------------------------------------------
# Add parameterization to PREFECT scripts

Pass parameters to the main function of the script and the parameters will be detected by PREFECT.

## PREFECT Deployment
Such a deployment can be used with Docker, Kubernetes, etc...

### Let's create the deployment .yaml file (we can edit the yaml if needed)
>>> prefect deployment build ./etl_web_to_gcs.py:etl_parent_flow -n "Parameterized ETL"

### Let's add some default parameter values.
---> Edit the .yaml file and replace parameters: {}
with parameters: {"color":"green", "months":[1,2,3], "year":2020}

### Let's apply the modifications
>>> prefect deployment apply etl_parent_flow-deployment.yaml 

### Deployment using Orion CLI
Go to Deployments and now there should be our deployment listed.
There we can edit the deployement, manually add it to the agent scheduler with default parameters (Quick run) or with special parameters (Custom run), etc.

### Let' trigger the deployment
Using the Orion GUI, add a "Quick run" on the deployment. It will appear as scheduled (ready to run), but it won't run until an **agent** takes it in charge.

### Let's run the agent (very light python process // cronjob)
>>> prefect agent start --work-queue "default"



## Full deployment using terminal commands
We can also set the **cronjob** (--cron) and **apply** (-a) when **building**
>>> prefect deployment build ./etl_web_to_gcs.py:etl_parent_flow -n "Parameterized ETL" --cron "*/5 * * * *" -a

# --------------------------------------------------------------
# Deploy pipeline using PREFECT and Docker

## Add a docker-requirements.txt file with the libs needed for the docker image

## Add a Dockerfile with the following content

> **********
> #FROM prefecthq/prefect:2.7.7-python3.9 
> 
> COPY docker-requirements.txt .
> 
> RUN pip install -r docker-requirements.txt --trusted-host pypi.python.org --no-cache-dir
> 
> # copy the current folder with scripts & data
> COPY ./* /opt/prefect/ 
> 
> # No need to add ENTRYPOINT or CMD
> **********

## Create & tag the docker image

>>> docker build -t valkea/prefect:v1 .

## Push the docker image to the DockerHub

>>> docker push valkea/prefect:v1

## Add PREFECT Docker Block

### PREFECT-GUI ## "Blocks" / "Add Block" / "Docker Container"

name ---> ny-taxi-docker
image ---> valkea/prefect:v1
ImagePullPolicy ---> ALWAYS
AutoRemove ---> True (just to keep the machine cleaner)
---> click CREATE

## Create a docker_deploy.py

---> Paste the code given by the Docker Block
---> See file docker_deploy.py content

## Install the docker deployment into the local PREFECT

>>> prefect config set PREFECT_API_URL="http://127.0.0.1:4200/api"

(alternatively we can use Prefect Cloud or any other url)
(and if needed we can unset this with : prefect config unset PREFECT_API_URL ) 

>>> python docker_deploy.py

## Run the deployement

---> use the "Quick run" or "Custom run" button in the GUI

OR

>>> prefect deployment run etl-parent-flow/docker-flow

OR with extra paremeters

>>> prefect deployment run etl-parent-flow/docker-flow -p "months=[1,2]"