from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StringType, StructField, StructType

# Importing constants from utils
from utils import constants

print("START ===============================================================")

# Initialize Spark session with MongoDB configuration
spark_session = (
    SparkSession.builder.appName("SparkApp")
    .config(
        "spark.mongodb.write.connection.uri",
        f"{constants.MONGO_URI}/{constants.MONGO_DB}.{constants.MONGO_COLLECTION}",
    )
    .getOrCreate()
)

# Define the schema for the JSON data with only one field
schema = StructType([StructField("molecular_trait_id", StringType(), True)])
print("schema")
print(schema)

# Read from Kafka topic
df = (
    spark_session.readStream.format("kafka")
    .option("kafka.bootstrap.servers", constants.BOOTSTRAP_SERVERS)
    .option("subscribe", constants.KAFKA_TOPIC_TRANSFORMED)
    .option("startingOffsets", "earliest")
    .load()
)

# Parse the Kafka value column into a JSON structure
parsed_df = df.selectExpr("CAST(value AS STRING) as json_value")
print("parsed_df")
print(parsed_df)

json_df = parsed_df.select(from_json(col("json_value"), schema).alias("data")).select(
    "data.*"
)
print("json_df")
print(json_df)

# Debugging: Print the schema and show some rows
json_df.printSchema()

# Debugging: Print the parsed data (for debugging)
json_df.writeStream.format("console").outputMode("append").start()

# Write the stream to MongoDB
query = (
    json_df.writeStream.format("mongodb")
    .option("checkpointLocation", "/tmp/checkpoints")
    .option(
        "uri",
        f"{constants.MONGO_URI}/{constants.MONGO_DB}.{constants.MONGO_COLLECTION}",
    )
    .outputMode("append")
    .start()
)

print("END ===============================================================")

# Await termination (will block until the query is stopped)
query.awaitTermination()
