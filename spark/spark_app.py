from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, from_json, lit
from pyspark.sql.types import (
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from utils import constants

# TODO: clean up debug logs
print("=== START ===")

spark_session = (
    SparkSession.builder.appName("SparkApp")
    .config(
        "spark.mongodb.write.connection.uri",
        f"{constants.MONGO_URI}/{constants.MONGO_DB}.{constants.MONGO_COLLECTION}",
    )
    .getOrCreate()
)

# Define the schema for the JSON data
schema = StructType(
    [
        StructField("molecular_trait_id", StringType(), True),
        StructField("molecular_trait_object_id", StringType(), True),
        StructField("chromosome", StringType(), True),
        StructField("position", IntegerType(), True),
        StructField("ref", StringType(), True),
        StructField("alt", StringType(), True),
        StructField("variant", StringType(), True),
        StructField("ma_samples", IntegerType(), True),
        StructField("maf", FloatType(), True),
        StructField("pvalue", FloatType(), True),
        StructField("beta", FloatType(), True),
        StructField("se", FloatType(), True),
        StructField("type", StringType(), True),
        StructField("aan", StringType(), True),
        StructField("r2", StringType(), True),
        StructField("gene_id", StringType(), True),
        StructField("median_tpm", FloatType(), True),
        StructField("rsid", StringType(), True),
        StructField("study_id", StringType(), True),
    ]
)
# print("schema")
# print(schema)

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
json_df = parsed_df.select(from_json(col("json_value"), schema).alias("data")).select(
    "data.*"
)

# Dynamic collection name based on study_id
json_df = json_df.withColumn("collection_name", concat(lit("study_"), col("study_id")))
# print("json_df")
# print(json_df)


# Write to MongoDB using the collection name from the batch DataFrame
def write_to_mongo_collection(batch_df, batch_id):
    for collection_name in batch_df.select("collection_name").distinct().collect():
        collection_name = collection_name["collection_name"]

        # Write each micro-batch to the corresponding collection
        batch_df.filter(batch_df.collection_name == collection_name).write.format(
            "mongodb"
        ).mode("append").option("database", constants.MONGO_DB).option(
            "collection", collection_name
        ).save()


# Apply the write operation to each micro-batch
query = (
    json_df.writeStream.foreachBatch(write_to_mongo_collection)
    .option("checkpointLocation", "/tmp/checkpoints")
    .start()
)

print("=== END ===")

query.awaitTermination()
