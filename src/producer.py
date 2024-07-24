from kafka import KafkaProducer
import time
import numpy as np
from utils.mysql_connection import *


# Start Producer Application
if __name__ == "__main__":
    # Load stream_data table form kafka_prod database
    df = table_df('kafka_prod', 'stream_data')

    # Assign Kafka topic name and bootstrap server address
    KAFKA_TOPIC_NAME_CONS = "SENDDATA"
    KAFKA_BOOTSTRAP_SERVERS_CONS = 'localhost:9092'

    print("Kafka Producer Application Started ... ")

    # Initialize KafkaProducer object with specified bootstrap server and value serializer
    kafka_producer_obj = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS_CONS,
        value_serializer=lambda x: x.encode('utf-8')
    )

    # Add an order_id column with unique identifiers for each row
    df['order_id'] = np.arange(len(df))

    # Convert the DataFrame to a list of dictionaries, each representing a record
    data = df.to_dict(orient="records")

    # Iterate through each record in the data
    for message in data:
        # Create a list and store field values of the message
        message_fields_value_list = [message["order_id"], message["account_number"], message["branch"],
                                     message["transaction_code"]]

        # Concatenate field values into a single string separated by commas
        message = ','.join(str(v) for v in message_fields_value_list)

        # Print the type and content of the message for debugging purposes
        print("Message Type: ", type(message))
        print("Message: ", message)

        # Send the message to the specified Kafka topic
        kafka_producer_obj.send(KAFKA_TOPIC_NAME_CONS, message)

        # Pause for a short time before sending the next message
        time.sleep(1)

    print("Kafka Producer Application Completed. ")
