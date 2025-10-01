
# Senior Data Engineering Assignment Solution

  

This repository contains a solution for a senior data engineering assignment. It demonstrates a real-time data streaming pipeline using **Python**, **PySpark**, **Kafka**, and **Cassandra**, all orchestrated with **Docker**.

  

## Technologies Used

  

- Python

- Docker & Docker Compose

- PySpark

- Cassandra

- Kafka

- Spark

  

## Prerequisites

  

- Docker installed on your machine

- Docker Compose

  

## Installation

  

1. Clone the repository:

```bash

git clone <repo_url>

cd <repo_name>

````

  

2. Start the Docker containers:

  

```bash

docker-compose up -d

```

  

## Usage

  

Once the containers are running, follow these steps:

  

### Terminal 1 – Stream data from Kafka to Cassandra

  

```bash

docker exec -it spark-master bash

spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7,com.datastax.spark:spark-cassandra-connector_2.12:3.4.1 code/kafka_to_cassandra_sensor_stream.py

```

  

### Terminal 2 – Query Cassandra

  

```bash

docker exec -it cassandra_db cqlsh -u cassandra -p cassandra

USE sensor_stream;

SELECT * FROM sensor_data;

```

  

> Note: If you don’t see data immediately, wait a few seconds as the streaming pipeline processes incoming data.

  

### Terminal 3 – Stream data from Kafka to Kafka

  

```bash

docker exec -it spark-master bash

spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7 code/kafka_to_kafka_sensor_stream.py

```

> Note: If you don’t see data immediately, wait a few seconds as the streaming pipeline processes incoming data.

### Web Interface

  

Open your web browser and visit:

[http://localhost:8080/ui/docker-kafka-server](http://localhost:8080/ui/docker-kafka-server)