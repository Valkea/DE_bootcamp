# Question 1. Load `January` `2020` `Green` data

> Using the **etl_web_to_gcs.py** flow that loads taxi data into GCS as a guide, create a flow that loads the **green** taxi CSV dataset for **January 2020** into GCS and run it. Look at the logs to find out how many rows the dataset has.
> How many rows does that dataset have?

>>> python etl_web_to_gcs.py --year 2020 --months 1 --color green
| INFO    | Task run 'Transform Data-7a5e1946-0' - row count: **447770**

> So the answer is **447770**

# Question 2. Scheduling with Cron

> Cron is a common scheduling specification for workflows.
> Using the flow in etl_web_to_gcs.py, create a deployment to run on the **first of every month at 5am UTC**. What’s the cron schedule for that?

The format is : *minute hours days month year*

> So the answer is -1 5 1 * *

# Question 3. Loading data to BigQuery

> Using **etl_gcs_to_bq.py** as a starting point, modify the script for extracting data from GCS and loading it into BigQuery. This new script **should not fill or remove rows with missing values**. (The script is really just doing the E and L parts of ETL).
> The main flow should **print the total number of rows processed** by the script. Set the flow decorator to log the print statement.
> **Parametrize the entrypoint** flow to accept a list of **months**, a **year**, and a taxi **color**.
> Make any other necessary changes to the code for it to function as required.
> **Create a deployment** for this flow to run in a **local subprocess** with **local flow code storage** (the defaults).
> Make sure you have the parquet data files for **Yellow** taxi data for **Feb. 2019** and **March 2019** loaded in GCS.
> Run your deployment to append this data to your BiqQuery table. How many rows did your flow code process?

## Grab the requested datasets and push them to the GCS
>>> python flows/homework/etl_web_to_gcs.py --year 2019 --months 2 3 --color yellow
| INFO    | Task run 'Transform Data-7a5e1946-0' - row count: 7019375
| INFO    | Task run 'Transform Data-7a5e1946-0' - row count: 7832545

> So the answer is : 7019375+7832545 = **14851920**

## Let's create a Big Query table to confirm ou result using this tool (just for the sake of doing the exercice)

#### GCP-UI ## "Big Query" / "Add data" / "Google Cloud Storage"
--> Browse to "week02_02_gcp_bucket/homework/*"
--> Project : lexical-passkey-375922
--> Dataset : create a new one with "homework_week2" and select it
--> Table : taxi-rides
--> Click CREATE TABLE

#### GCP-UI ## "Big Query" / "Explorer" 
>>> SELECT COUNT(1) FROM `lexical-passkey-375922.homework_week2.taxi-rides`;
Row 14851920

> So the answer is **14851920** again

## Let's use the etl_gcs_to_bq.py to ty another way

Let's ensure that the table is empty
>>> DELETE FROM `lexical-passkey-375922.homework_week2.taxi-rides` WHERE true;

Then let's use the modified etl_gcs_to_bq.py 
>>> python flows/homework/etl_gcs_to_bq.py --year 2019 --months 2 3 --color yellow
	
#### GCP-UI ## "Big Query" / "Explorer" 
>>> SELECT COUNT(1) FROM `lexical-passkey-375922.homework_week2.taxi-rides`;
Row 14851920

> So the answer is **14851920** again