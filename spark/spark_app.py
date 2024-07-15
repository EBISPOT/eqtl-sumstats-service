from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

# Importing constants from utils
from utils import constants

# Initialize Spark session with MongoDB configuration
spark_session = SparkSession.builder \
    .appName("SparkApp") \
    .config("spark.mongodb.write.connection.uri", f"{constants.MONGO_URI}/{constants.MONGO_DB}.{constants.MONGO_COLLECTION}") \
    .getOrCreate()

# Read from Kafka topic
df = spark_session.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", constants.BOOTSTRAP_SERVERS) \
    .option("subscribe", constants.KAFKA_TOPIC_TRANSFORMED) \
    .load()

# Define the schema for the Kafka messages
schema = StructType([
    StructField("key", StringType(), True),
    StructField("value", StringType(), True)
])

# Parse the Kafka key and value columns into strings
parsed_df = df.selectExpr("CAST(key AS STRING) as key", "CAST(value AS STRING) as value")

print('1~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')

# Debugging: Print the schema and show some rows
parsed_df.printSchema()

print('2~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')

# Print the parsed data (for debugging)
parsed_df.writeStream \
    .format("console") \
    .outputMode("append") \
    .start()


print('3~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')

# Write the stream to MongoDB
query = parsed_df.writeStream \
    .format("mongodb") \
    .option("checkpointLocation", "/tmp/checkpoints") \
    .option("uri", f"{constants.MONGO_URI}/{constants.MONGO_DB}.{constants.MONGO_COLLECTION}") \
    .outputMode("append") \
    .start()

# Await termination (will block until the query is stopped)
query.awaitTermination()
