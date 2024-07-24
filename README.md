# Kafka Streaming Project

This project demonstrates how to use Kafka for streaming data between a MySQL database and Kafka topics. It includes options for both encrypted and unencrypted data transfers.

## Prerequisites

1. **Install MySQL**: Ensure MySQL is installed on your host machine.
2. **Install Kafka**: Ensure Kafka is installed on your host machine.

## Setup Kafka

1. **Start Zookeeper**:
```bash
   <location_to_kafka/bin>/zookeeper-server-start.sh <location_to_kafka/config>/zookeeper.properties
```
2. **Start Kafka Broker**:
```bash
   <location_to_kafka/bin>/kafka-server-start.sh <location_to_kafka/config>/server.properties
```
## Configuration

- Update .env File:
  Make sure to update the .env file with your MySQL credentials and encryption key.

## Running the Project
Depending on whether you want to use encryption or not, follow the appropriate steps:

### Without Encryption
1. Run Producer: Execute the producer script or notebook to transfer data from the database to a Kafka topic.
2. Run Consumer: Execute the consumer script or notebook to transfer data from the Kafka topic to the database.

### With Encryption

1. Run Producer with Encryption: Execute the producer_encrypt script or notebook.
2. Run Consumer with Encryption: Execute the consumer_encrypt script or notebook.
  
