import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, lit
from pyspark.sql.types import (
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from data_extraction import data_extraction
from utils import constants


def process_file(study_id, dataset_id, df):
    # Add study_id and collection_name as new columns
    df = df.withColumn("study_id", lit(study_id))
    df = df.withColumn("collection_name", concat(lit("study_"), col("study_id")))

    # Get distinct collection names
    distinct_collections = df.select("collection_name").distinct().collect()

    # Write data to MongoDB by collection name
    for row in distinct_collections:
        collection_name = row["collection_name"]
        write_to_mongo(df, collection_name)


def write_to_mongo(df, collection_name):
    print(f"Writing to collection: {collection_name}")
    df.filter(df.collection_name == collection_name).write.format("mongodb").mode(
        "append"
    ).option("database", constants.MONGO_DB).option(
        "collection", collection_name
    ).save()


# Initialize SparkSession
spark = (
    SparkSession.builder.appName("SparkApp")
    .config(
        "spark.mongodb.write.connection.uri",
        f"{constants.MONGO_URI}/{constants.MONGO_DB}.{constants.MONGO_COLLECTION}",
    )
    .getOrCreate()
)


schema = StructType(
    [
        StructField("molecular_trait_id", StringType(), True),
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
        StructField("ac", StringType(), True),
        StructField("an", StringType(), True),
        StructField("r2", StringType(), True),
        StructField("molecular_trait_object_id", StringType(), True),
        StructField("gene_id", StringType(), True),
        StructField("median_tpm", FloatType(), True),
        StructField("rsid", StringType(), True),
    ]
)


ftp = data_extraction.connect_ftp()
files_to_etl = data_extraction.get_files_to_etl()

# TODO: run this parallel
for study_id, dataset_id, file_name in files_to_etl:
    print(f"""---filepath: {file_name}---""")

    # DOWNLOAD AND READ ##############################
    file_path_remote = f"{constants.FTP_BASE_PATH}{study_id}/{dataset_id}/{file_name}"
    file_path_local = os.path.join(constants.LOCAL_PATH, file_name)
    data_extraction.download_file(ftp, file_path_remote, file_path_local)
    df = spark.read.csv(file_path_local, sep="\t", header=True, schema=schema)

    print(f"Processing {study_id}/{dataset_id}/{file_name}")
    process_file(study_id, dataset_id, df.limit(10))  # TODO: remove `.limit(10)`
    print(f"Done processing {study_id}/{dataset_id}/{file_name}")

    try:
        os.remove(file_path_local)
        print(f"File {file_path_local} has been removed after processing.")
    except OSError as e:
        print(f"Error removing file {file_path_local}: {e}")

print("=== ETL Process Complete ===")
