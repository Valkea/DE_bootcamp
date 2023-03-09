
In order to use this project we need to do the following:

## Virtual environment
let's create a virtual environment and install the required Python libraries

(Linux or Mac)
```bash
>>> python3 -m venv venvKafka
>>> source venvKafka/bin/activate
>>> pip install -r requirements.txt
```

(Windows):
```bash
>>> py -m venv venvkafka
>>> .\venvkafka\Scripts\activate
>>> py -m pip install -r requirements.txt
```

## Running Spark and Kafka Clusters on Docker

### 1. Build Required Images for running Spark

```bash
>>> ./docker/spark/bash.sh
```

### 2. Create Docker Network & Volume

```bash
>>> docker network  create kafka-spark-network
>>> docker volume create --name=hadoop-distributed-file-system
```

### 3. Run Kafka Services on Docker
```bash
>>> cd docker/kafka
>>> docker compose up -d
```

### 4. Playing with json_example / avo_example / streams-example

#### JSON_example
```bash
>>> cd json_example
>>> python producer.py # from terminal 1
>>> python consumer.py # from terminal 2
```

#### AVRO_example (avro is an encoding protocol that optimize the network exchanges)
```bash
>>> cd avro_example
>>> python producer.py # from terminal 1
>>> python consumer.py # from terminal 2
```

### 5. Run Kafka Services on Docker
```bash
>>> cd docker/spark
>>> docker compose up -d
```

### 6. Playing with Streams-example

```bash
>>> cd streams-example/pyspark
>>> python producer.py # from terminal 1
>>> python consumer.py # from terminal 2
```

Then in order to use the stream

```bash
>>> cd streams-example/pyspark
>>> ./spark-submit.sh streaming.py # from terminal 1
>>> python producer.py # from terminal 2
```
(I wasn't able to run the streaming.py because of a problem with the Spark version)


### 7. Stop Services on Docker
```bash
>>> cd ../kafka
>>> docker compose down
>>> cd ../spark
>>> docker compose down
```



## Extra Commands

### Create topic

```bash
./bin/kafka-topics.sh --create --topic demo_1 --bootstrap-server localhost:9092 --partitions 2
```

### Check docker's networks
```bash
>>> docker network ls
```

#### Check docker's volumes
```bash
>>> docker volume ls
```

### Delete all Containers
```bash
>>> docker rm -f $(docker ps -a -q)
``` 

### Delete all volumes
```bash
docker volume rm $(docker volume ls -q)
```
