
In order o run SPARK we will use a Google Cloud Virtual Machine (see week01 / connect_GCP_insance / commands.md)
And we will follow the instructions available here: https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/week_5_batch_processing/setup/linux.md

## Start & connect to GC-VM

--> Go to Google-Cloud console / "Compute Engine" / "VM instances"
--> Start the VM instance you intend to connect with
--> Copy the newly attributed extenral IP

--> On the local machine, edit ~/.ssh/config 
--> Replace the old IP address with the new one

--> connect to the remote server
```bash
>>> ssh de-zoomcamp
``` 

## Install Java (because SPARK needs java)

Let's create a spark folder to gather all the relative files in the same folder

```bash
>>> mkdir spark
>>> cd spark
```

Let's download, unzip Java

```bash
>>> wget https://download.java.net/java/GA/jdk11/9/GPL/openjdk-11.0.2_linux-x64_bin.tar.gz
>>> tar xzfv openjdk-11.0.2_linux-x64_bin.tar.gz
>>> rm openjdk-11.0.2_linux-x64_bin.tar.gz
```

Then let's create en environment variable requiered by SPARK to access Java

```bash
>>> export JAVA_HOME="${HOME}/spark/jdk-11.0.2"
>>> export PATH="${JAVA_HOME}/bin:${PATH}"
```

Finally we can check if java is installed

```bash
>>> java --version
```

## Install Spark

Let's download Spark (3.3.0 because newer versions are instable with Linux)
(check available version here https://spark.apache.org/downloads.html / select "Pre-built for Apache Hadoop...")

```bash
[in spark folder]
>>> wget https://dlcdn.apache.org/spark/spark-3.3.2/spark-3.3.2-bin-hadoop3.tgz
>>> tar xzfv spark-3.3.2-bin-hadoop3.tgz 
>>> rm spark-3.3.2-bin-hadoop3.tgz 
```

Then let's create en environment variable to start *spark-shell*

```bash
>>> export SPARK_HOME="${HOME}/spark/spark-3.3.2-bin-hadoop3"
>>> export PATH="${SPARK_HOME}/bin:${PATH}"
```

Finally we can check if it works by starting spark-shell

```bash
>>> spark-shell

scala> val data = 1 to 10000
data: scala.collection.immutable.Range.Inclusive = Range 1 to 10000

scala> val distData = sc.parallelize(data)
distData: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[0] at parallelize at <console>:24

scala> distData.filter(_ < 10).collect()
res0: Array[Int] = Array(1, 2, 3, 4, 5, 6, 7, 8, 9)  

scale> :quit
```

## Perenize EXPORTS
In order to avoid EXPORTing variables each time we start the VM, we will save them in the bashrc file

```bash
vim ~/.bashrc
```

Add the following at the bottom

```
# Spark exports
export JAVA_HOME="${HOME}/spark/jdk-11.0.2"
export PATH="${JAVA_HOME}/bin:${PATH}"
export SPARK_HOME="${HOME}/spark/spark-3.3.2-bin-hadoop3"
export PATH="${SPARK_HOME}/bin:${PATH}"
``` 

Restart the ssh connexion

```bash
>>> exit
>>> ssh de-zoomcamp
```

## Configure PySpark for Jupyter notebook (both are on the GC-VM)

```bash
>>> cd .. (to root of the VM)
>>> mkdir notebooks
>>> cd notebooks
```

Let's download some data for the tests

```bash
>>> wget https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv
```

To run PySpark, we first need to add it to PYTHONPATH:

```bash
>>> ls ../spark/spark-3.3.2-bin-hadoop3/python/lib/ (to see the exact version)
>>> export PYTHONPATH="${SPARK_HOME}/python/:$PYTHONPATH"
>>> export PYTHONPATH="${SPARK_HOME}/python/lib/py4j-0.10.9.5-src.zip:$PYTHONPATH"
>>> pip install pyspark

>>> jupyter notebook
```

## Configure port forwarding using Visual Studio Code (to access the remote jupyter & spark instances locally)

# Connect a remote SSH server as explained above
# Go to the Terminal and select tab "PORTS"
# Add 8888 for jupyter notebooks (we can execute the notebook on the GCP server and access it locally !)
# Add 4040 for Spark Master (http://localhost:4040/jobs/)

--> Open the given jupyter URL
--> Create a notebook

```
import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[*]") \ # * = all CPUS
    .appName('test') \
    .getOrCreate()

df = spark.read \
    .option("header", "true") \
    .csv('taxi+_zone_lookup.csv')

df.show()

# Write parqet file to 'zones' folder
df.write.parquet('zones')
```

We should now see a zones folder with a parquet file inside along with a _SUCCESS file


### We can finally access the Spark Master (because we already forwarded port 4040):

Go to http://localhost:4040/jobs/


# Preparing Yellow and Green Taxi Data

Let's download the data from the repository

```bash
>>> ./download_data.sh yellow 2020
>>> ./download_data.sh yellow 2021
>>> ./download_data.sh green 2020
>>> ./download_data.sh green 2021
```

Then let's use the 05_taxi_schema.ipynb (from GC-VM) to split each file into 4 paquet files


# Sending data to GCS with command line (video 5.6.1)

```bash
>>> gsutil -m cp -r data/pq gs://week02_02_gcp_bucket/week5/pq
```

# Connect Spark to GCS

## 1. IMPORTANT: Download the Cloud Storage connector for Hadoop here: https://cloud.google.com/dataproc/docs/concepts/connectors/cloud-storage#clusters
(As the name imply, this .jar file is essentially what connects Spark with GCS)

```bash
>>> mkdir lib
>>> cd lib
>>> wget https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar
```

## 2. We must upload our GCS passkey.json file

```bash
>>> sftp de-zoocamp
>>> cd /home/valkea
>>> mkdir .google
>>> cd .google
>>> mkdir credentials
>>> cd credentials
>>> put lexical-passkey-375922-eec6cf60b4f3.json
```

## 3. In the python script we need to make some extra imports

```
import pyspark
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
```

## And adapt the code...

```
credentials_location = '/home/valkea/.google/credentials/lexical-passkey-375922-eec6cf60b4f3.json'

conf = SparkConf() \
    .setMaster('local[*]') \
    .setAppName('test') \
    .set("spark.jars", "./lib/gcs-connector-hadoop3-latest.jar") \
    .set("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", credentials_location)

sc = SparkContext(conf=conf)

hadoop_conf = sc._jsc.hadoopConfiguration()
hadoop_conf.set("fs.AbstractFileSystem.gs.impl",  "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
hadoop_conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
hadoop_conf.set("fs.gs.auth.service.account.json.keyfile", credentials_location)
hadoop_conf.set("fs.gs.auth.service.account.enable", "true")

spark = SparkSession.builder \
    .config(conf=sc.getConf()) \
    .getOrCreate()
```

Then

```
df_green = spark.read.parquet('gs://week02_02_gcp_bucket/week5/data/pq/green/*/*')
```

# Creating a local Spark Cluster & use Start-commit

## Let's convert our notebook to python script

Let's start the master
```bash
>>> cd ~/spark/spark-3.3.2-bin-hadoop3/
>>> ./sbin/start-master.sh
```

Go to http://localhost:8080 inorder to grab the Spark Master local URL (spark://de-zoomcamp.europe-west1-b.c.lexical-passkey-375922.internal:7077)

```bash
>>> ./sbin/start-worker.sh spark://de-zoomcamp.europe-west1-b.c.lexical-passkey-375922.internal:7077
```

Now we can connect using a python script or a jupyter notebook (configured with spark://de-zoomcamp.europe-west1-b.c.lexical-passkey-375922.internal:7077)
or check the Sparkmaster on the usual URL http://localhost:4040

Let's convert the configured notebook to a python script

```bash
>>> cd ~/week_5_batch_processing/code/
>>> jupyter nbconvert --to=script 10_use_existing_Local_Spark_Cluster.ipynb
```

Let's run the python script directly (if the ressources are not avaible, the notebook must be shut down)

```bash
>>> python 10_use_existing_Local_Spark_Cluster.py \
    -g 'data/pq/green/2020/*/' \
    -y 'data/pq/yellow/2020/*/' \
    -o 'data/report/2020'
```

```bash
>>> cd ~/week_5_batch_processing/code/
>>> spark-submit \
    --master='spark://de-zoomcamp.europe-west1-b.c.lexical-passkey-375922.internal:7077' \
        10_use_existing_Local_Spark_Cluster_and_spark_submit.py \
            -g 'data/pq/green/2020/*/' \
            -y 'data/pq/yellow/2020/*/' \
            -o 'data/report/2020'
```


Finally, once we are finished, we need to stop the worker and the master

```bash
>>> cd ~/spark/spark-3.3.2-bin-hadoop3/
>>> ./sbin/stop-worker.sh
>>> ./sbin/stop-master.sh
```

# Setting up a GC-Dataproc Spark Cluster

## In GC-UI search for Dataproc and ENABLE it

--> CREATE CLUSTER

--> Create a cluster on compute engine
---> name: cluster-de-zoomcamp
---> region: us-central1 (the same as the bucket we will work with)
---> cluster type: standard
---> components: Jupyter notebook, Docker (not required for this project, but might be useful)
--> Config cluster specs
--> CREATE

A new VM is created for the Cluster

## Let's upload the python script to the GCS bucket
```bash
>>> cd ~/week_5_batch_processing/code/
>>> gsutil cp 10_use_existing_Local_Spark_Cluster_and_spark_submit.py \
    gs://week02_02_gcp_bucket/week5/code/10_use_existing_Local_Spark_Cluster_and_spark_submit.py
```
 
## Back to the GC-DataProc page
--> Click the cluster name

--> SUBMIT JOBS
---> job type: pyspark
---> main python file: gs://week02_02_gcp_bucket/week5/code/10_use_existing_Local_Spark_Cluster_and_spark_submit.py
---> arguments:
            -g=gs://week02_02_gcp_bucket/week5/data/pq/green/2021/*/
            -y=gs://week02_02_gcp_bucket/week5/data/pq/yellow/2021/*/
            -o=gs://week02_02_gcp_bucket/week5/data/report/2021
---> SUBMIT

After the Job execution, we should see a new report in the Bucket


## We can do the very same using the google cloud CLI (gsutil)

First add the 'DataProc Admin' persmission to the associated IAM account
Then:

```bash
>>> gcloud dataproc jobs submit pyspark \
    --cluster=cluster-de-zoomcamp \
    --region=us-central1 \
    gs://week02_02_gcp_bucket/week5/code/10_use_existing_Local_Spark_Cluster_and_spark_submit.py \
    -- \
        -g=gs://week02_02_gcp_bucket/week5/data/pq/green/2021/*/ \
        -y=gs://week02_02_gcp_bucket/week5/data/pq/yellow/2021/*/ \
        -o=gs://week02_02_gcp_bucket/week5/data/report/2021
```
(We can run such a command with a cronjob, from DBT or any equivalent...)

## We can also use the API calls... and many other ways (see doc)

## Finally let's SHUTDOWN 
--> the CLUSTER (on GC-DataProc page)
--> the Virtual Machines (on the VM instances page)


# Homework

## Question 1. Install Spark and PySpark
> - Install Spark
> - Run PySpark
> - Create a local spark session
> - Execute spark.version

> What's the output?

#### The answer is **res0: String = 3.3.2**

## Question 2: HVFHW June 2021

> - Read it with Spark using the same schema as we did in the lessons.
> - We will use this dataset for all the remaining questions.
> - Repartition it to 12 partitions and save it to parquet.

> What is the average size of the Parquet (ending with .parquet extension) Files that were created (in MB)?

Let's download the data from the repository

```bash
>>> cd ~/week_5_batch_processing/code
>>> ./download_data.sh fhvhv 2021
```

Let's execute the Homework notebook (the answers to questions 2, 3, 4, 5, 6 are saved in the notebook)