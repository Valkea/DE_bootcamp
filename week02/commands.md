## Install PREFECT and other libs

python -m venv venvWeek01

pip install -r requirements.txt

prefect version



## Edit ingest_data.py

### import flow and task from prefect
### create a flow function to organize the tasks (let's use the main function)
### create a task function (let's convert the ingest function) 

### Split the original function into ETL (Extract, Transform, Load) steps



## Run PREFECT GUI

prefect orion start

### open http://127.0.0.1:4200/



# Use PREFECT compatible libs (pre-configured to run with PREFECT)
https://docs.prefect.io/collections/catalog/

## 1. Let's externalize the SQLALCHEMY parameters from our script

pip install prefect-sqlalchemy

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


