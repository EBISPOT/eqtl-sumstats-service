from pyspark.sql import SparkSession

# Importing constants from utils
from utils import constants

# # Set log level to WARN
# SparkContext.setSystemProperty('log4j.rootCategory', 'WARN, console')

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

# Read from Kafka topic
df = (
    spark_session.readStream.format("kafka")
    .option("kafka.bootstrap.servers", constants.BOOTSTRAP_SERVERS)
    .option("subscribe", constants.KAFKA_TOPIC_TRANSFORMED)
    .option("startingOffsets", "earliest")
    .load()
)

# Parse the Kafka key and value columns into strings
parsed_df = df.selectExpr(
    "CAST(key AS STRING) as key", "CAST(value AS STRING) as value"
)

# Debugging: Print the schema and show some rows
parsed_df.printSchema()

# Print the parsed data (for debugging)
parsed_df.writeStream.format("console").outputMode("append").start()

# Write the stream to MongoDB
query = (
    parsed_df.writeStream.format("mongodb")
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
