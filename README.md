# Kafka Spark Data Encryption and Decryption

This project demonstrates how to produce and consume data with encryption and decryption using Apache Kafka and Apache Spark.

## Introduction

The project consists of several components:

- **Producer (`producer.ipynb`)**: This notebook contains code for producing data.

- **Consumer (`consumer.ipynb`)**: This notebook demonstrates reading data produced by the producer.



## Requirements

- Apache Kafka
- Apache Spark
- Python 3.x
- PySpark
- Kafka-python

## Usage

1. Install MySQL and kafka on the host machine. 
2. Excute the following commands in terminal: `<location to kafka/bin folder>/zookeeper-server-start.sh <location to kafka/config folder>/zookeeper.properties` and `<location to kafka/bin folder>/kafka-server-start.sh <location to kafka/config folder>/server.properties`
3. Update .env file with your credentials.
4. Run the Producer notebook (`producer.ipynb`) to produce data and send it through specified topic.
5. Run the Consumer notebook (`consumer.ipynb`) to consume the sent data.

