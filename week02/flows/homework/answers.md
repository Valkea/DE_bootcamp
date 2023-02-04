# Question 1. Load `January` `2020` `Green` data

> Using the **etl_web_to_gcs.py** flow that loads taxi data into GCS as a guide, create a flow that loads the **green** taxi CSV dataset for **January 2020** into GCS and run it. Look at the logs to find out how many rows the dataset has.
> How many rows does that dataset have?

>>> python etl_web_to_gcs.py --year 2020 --months 1 --color green
| INFO    | Task run 'Transform Data-7a5e1946-0' - row count: **447770**

> So the answer is **447770**
