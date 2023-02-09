# Question 1. Load `January` `2020` `Green` data

> Using the **etl_web_to_gcs.py** flow that loads taxi data into GCS as a guide, create a flow that loads the **green** taxi CSV dataset for **January 2020** into GCS and run it. Look at the logs to find out how many rows the dataset has.
> 
> How many rows does that dataset have?

>>> python etl_web_to_gcs.py --year 2020 --months 1 --color green
| INFO    | Task run 'Transform Data-7a5e1946-0' - row count: **447770**

> So the answer is **447770**

# Question 2. Scheduling with Cron

> Cron is a common scheduling specification for workflows.
> Using the flow in etl_web_to_gcs.py, create a deployment to run on the **first of every month at 5am UTC**.
>
> What’s the cron schedule for that?

The format is : *minute hours days month year*

> So the answer is 0 5 1 * *

# Question 3. Loading data to BigQuery

> Using **etl_gcs_to_bq.py** as a starting point, modify the script for extracting data from GCS and loading it into BigQuery. This new script **should not fill or remove rows with missing values**. (The script is really just doing the E and L parts of ETL).
> The main flow should **print the total number of rows processed** by the script. Set the flow decorator to log the print statement.
> **Parametrize the entrypoint** flow to accept a list of **months**, a **year**, and a taxi **color**.
> Make any other necessary changes to the code for it to function as required.
> **Create a deployment** for this flow to run in a **local subprocess** with **local flow code storage** (the defaults).
> Make sure you have the parquet data files for **Yellow** taxi data for **Feb. 2019** and **March 2019** loaded in GCS.
> Run your deployment to append this data to your BiqQuery table.
>
> How many rows did your flow code process?

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

# Question 4. Github Storage Block

> Using the **etl_web_to_gcs** script from the videos as a guide, you want to **store your flow code in a GitHub repository** for collaboration with your team. Prefect can look in the GitHub repo to find your flow code and read it.
> **Create a GitHub storage block** from the UI or in Python code and use that in your Deployment instead of storing your flow code locally or baking your flow code into a Docker image.
> Note that you will have to push your code to GitHub, Prefect will not push it for you.
> Run your deployment in a **local subprocess** (the default if you don’t specify an infrastructure). Use the **Green** taxi data for the month of **November 2020**.
> 
> How many rows were processed by the script?

> You need to create a GitHub repo and push all the files your flow needs to it. Then create your GitHub storage block that references that repo. Then build your deployment that specifies the storage block to use. Does that help?

## Grab the requested datasets and push them to the GCS
>>> python flows/homework/etl_web_to_gcs.py --year 2020 --months 11 --color green
| INFO    | Task run 'Transform Data-7a5e1946-0' - row count: 88605

> So the answer is : 7019375+7832545 = **88605**


### Let's install the GibHub Block

>>> pip install prefect-github
>>> prefect block register -m prefect_github



### Create the GitHub Block

#### Using a script
>>> python github_block.py

#### OR Using the Orion's ## "Blocks" / "Add Block" / "GitHub"
name ---> ny-taxi-github
repository --> https://github.com/Valkea/DE_bootcamp/tree/main/week02/flows/04_dockerization_and_deployment
---> click CREATE

### Push the target folder (homework) and target script (etl_web_to_gcs.py) to GitHub

### Create a PREFECT deployment

#### Using a script
>>> python guthub_deploy.py

#### OR Using a comand line
>>> prefect deployment build week02/flows/homework/etl_web_to_gcs.py:etl_parent_flow --name github-flow --tag dev -sb github/ny-taxi-github -a

### Create a new RUN 

#### Using the Orion's UI (Quick Run / Custom Run)
#### OR Using a command line
>>> prefect deployment run etl-parent-flow/github-flow

`/!\ We can rename the etl_web_togcs.py local script to ensure that the Github version is really used !`

# Question 5. Email or Slack notifications

> It’s often helpful to be notified when something with your dataflow doesn’t work as planned. Choose one of the options below for creating email or slack notifications.
> The hosted Prefect Cloud lets you avoid running your own server and has Automations that allow you to get notifications when certain events occur or don’t occur.
> Create a free forever Prefect Cloud account at app.prefect.cloud and connect your workspace to it following the steps in the UI when you sign up.
> Set up an Automation that will send yourself an email when a flow run completes. Run the deployment used in Q4 for the **Green** taxi data for **April 2019**. Check your email to see the notification.
> Alternatively, use a Prefect Cloud Automation or a self-hosted Orion server Notification to get notifications in a Slack workspace via an incoming webhook.
> You can grab the webhook URL from your own Slack workspace and Slack App that you create.
> 
> How many rows were processed by the script?

## Grab the requested datasets and push them to the GCS
>>> python flows/homework/etl_web_to_gcs.py --year 2019 --months 4 --color green
| INFO    | Task run 'Transform Data-7a5e1946-0' - row count: 514392

> So the answer is : **514392**

## Let's try the PREFECT Cloud

### Create a PREFECT cloud account

### Duplicate the GCP Creds and GCP Bucket blocks on the cloud

### Login from local terminal 
>>> prefect cloud login

### Install the GitHub Blocks

#### Using a script
>>> python github_block.py

#### OR Using the Orion's ## "Blocks" / "Add Block" / "GitHub"
name ---> ny-taxi-github
repository --> https://github.com/Valkea/DE_bootcamp/tree/main/week02/flows/04_dockerization_and_deployment
---> click CREATE

### Add an email block for notifications

### Add a notification alert using the "Automation" section

### Create a new RUN 

#### Using the Orion's UI (Quick Run / Custom Run)
#### OR Using a command line
>>> prefect deployment run etl-parent-flow/github-flow

### Start an Agent (the cloud needs a local agent, it won't run the agent alone)
>>> prefect agent start --work-queue "default" 

### Logout from the cloud
>>> prefect cloud logout

### Now the deployment will execute and send the notifications depending on the settings

# Question 6. Secrets

> Prefect Secret blocks provide secure, encrypted storage in the database and obfuscation in the UI. Create a secret block in the UI that stores a fake 10-digit password to connect to a third-party service.
>
> Once you’ve created your block in the UI, how many characters are shown as asterisks (*) on the next page of the UI?

### PREFECT-GUI ## "Blocks" / "Add Block" / "Secret"
--> input: 1234567890
******** --> **8**