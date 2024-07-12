from pyspark.sql import SparkSession

from utils import constants

spark_session = SparkSession.builder.appName("SparkApp").getOrCreate()

df = (
    spark_session.readStream.format("kafka")
    .option("kafka.bootstrap.servers", constants.KAFKA_SERVER)
    .option("subscribe", constants.KAFKA_TOPIC_TRANSFORMED)
    .load()
)

print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
print(df)
print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
