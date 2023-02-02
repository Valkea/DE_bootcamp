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


#--------------------------------------------------------------
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
