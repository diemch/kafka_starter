


from pyspark.sql import SparkSession
from datetime import datetime, timedelta
import time

spark = SparkSession.builder \
        .appName("My Spark Job") \
        .config("spark.jars", "/Users/diegomch/Downloads/jars/jar_files/*") \
        .config("spark.driver.extraClassPath", "/Users/diegomch/Downloads/jars/jar_files/*") \
        .getOrCreate()

# We will keep the consumer running for 10 secondsspark-sql-kafka-0-10_2.13-4.0.0-preview2.jar
stop_time = datetime.now() + timedelta(seconds=10)  # 10 seconds from now

##
## TODO TO UPDATE kafka.sasl.jaas.config
##
kafka_options = {
    "kafka.bootstrap.servers": "localhost:9092",
    # "kafka.sasl.mechanism": "SCRAM-SHA-256",
    # "kafka.security.protocol": "SASL_SSL",
    # "kafka.sasl.jaas.config": """org.apache.kafka.common.security.scram.ScramLoginModule required username="XXX" password="YYY";""",
    "startingOffsets": "earliest", # Start from the beginning when we consume from kafka
    "subscribe": "hello"           # Our topic name
}
#
# # Subscribe to Kafka topic "hello"
# df = spark.readStream.format("kafka").options(**kafka_options).load()

df = ( spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092") # Replace with your Kafka broker(s)
    .option("subscribe", "test-topic") # Replace with your topic name
    .option("startingOffsets", "earliest")# Start reading from the beginning
    .load() )

# df = spark \
#     .readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", "localhost:9092") \
#     .option("subscribe", "test-topic") \
#     .option("startingOffsets", "beginning") \
#     .load()

# Deserialize the value from Kafka as a String for now
deserialized_df = df.selectExpr("CAST(value AS STRING)")

# Query Kafka and wait 10sec before stopping pyspark
query = deserialized_df.writeStream.outputMode("append").format("console").start()
time.sleep(12)
query.stop()