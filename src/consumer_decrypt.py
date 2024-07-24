from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from utils.mysql_connection import *
import time

# Define Kafka topic name and bootstrap server address
kafka_topic_name = 'SENDDATAENC'
kafka_bootstrap_servers = 'localhost:9092'
KEY = os.getenv('KEY')
# Initialize a Spark session with Kafka support
spark = SparkSession \
    .builder \
    .appName("Structured Streaming") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .getOrCreate()

# Set logging level to ERROR to reduce verbosity
spark.sparkContext.setLogLevel("ERROR")

# Read streaming data from Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic_name) \
    .option("startingOffsets", "earliest") \
    .load()

# Print the schema of the incoming Kafka messages
df.printSchema()

# Select the 'value' and 'timestamp' fields from the Kafka message and cast 'value' to string
df1 = df.selectExpr("CAST(value AS STRING)", "timestamp")

# Define the schema of the incoming CSV data
df_schema_string = "order_id INT, encrypted_account_number STRING, branch STRING, transaction_code STRING"

# Parse the 'value' field from CSV format into individual columns based on the schema
df2 = df1 \
    .select(from_csv(col("value"), df_schema_string) \
    .alias("data"), "timestamp")

# Flatten the 'data' structure to select individual fields along with the timestamp
df3 = df2.select("data.*", "timestamp")
# Decrypt the encrypted account number column
df3 = df3.withColumn('decrypted_account_number', expr(f"CAST(aes_decrypt(unbase64(encrypted_account_number), '{KEY}', 'ECB', 'PKCS') AS STRING)".format())
)
# Create a temporary view to allow for SQL queries on the processed data
df3.createOrReplaceTempView("proc_rw_transaction_data_enc")

# Execute an SQL query to select all data from the temporary view
data = spark.sql("SELECT * FROM proc_rw_transaction_data_enc")

# Write the streaming query results to an in-memory table for further processing or visualization
data_agg_write_stream = data \
    .writeStream \
    .trigger(processingTime='5 seconds') \
    .outputMode("append") \
    .option("truncate", "false") \
    .format("memory") \
    .queryName("temp_stream_data_enc") \
    .start()

# Await termination for a short period to allow the stream to start processing
data_agg_write_stream.awaitTermination(1)

count = -1
while True:
# Write the results of query to dataframe
    df = spark.sql("SELECT decrypted_account_number, branch, transaction_code, timestamp FROM temp_stream_data_enc")
    data = df.toPandas()
    if len(data) > count:
        time.sleep(5)
        df_table(data,'kafka_con','stream_data')
        count = len(data)
    else:
        break