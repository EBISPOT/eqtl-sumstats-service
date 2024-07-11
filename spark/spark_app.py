from pyspark.sql import SparkSession

from constants import KAFKA_SERVER, KAFKA_TOPIC

spark_session = SparkSession.builder.appName("SparkApp").getOrCreate()

df = (
    spark_session.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_SERVER)
    .option("subscribe", KAFKA_TOPIC)
    .load()
)

print("===========================================================")
print(df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)"))
print("===========================================================")
