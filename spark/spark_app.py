import concurrent.futures
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.types import (
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from utils import utils
from utils import constants


def process_file(study_id, dataset_id, file_name):
    print(f"""---filepath: {file_name}---""")

    file_path_remote = f"{constants.FTP_BASE_PATH}{study_id}/{dataset_id}/{file_name}"
    file_path_local = os.path.join(constants.LOCAL_PATH, file_name)
    try:
        utils.update_etl_date(
            study_id, dataset_id, file_name, constants.ETLStatus.DOWNLOAD_IN_PROGRESS
        )
        utils.download_file(ftp, file_path_remote, file_path_local)
        utils.update_etl_date(
            study_id, dataset_id, file_name, constants.ETLStatus.DOWNLOAD_COMPLETED
        )
    except Exception as e:
        print(f"Failed downloading {study_id}/{dataset_id}/{file_name}: {e}")
        utils.update_etl_date(
            study_id, dataset_id, file_name, constants.ETLStatus.DOWNLOAD_FAILED
        )
        raise

    try:
        utils.update_etl_date(
            study_id, dataset_id, file_name, constants.ETLStatus.EXTRACTION_IN_PROGRESS
        )
        df = spark.read.csv(file_path_local, sep="\t", header=True, schema=schema)
        # TODO: remove this
        df = df.limit(10)

        print(f"Processing {study_id}/{dataset_id}/{file_name}")
        df = df.withColumn("study_id", lit(study_id))
        df = df.withColumn("dataset_id", lit(dataset_id))
        df = df.withColumn("file_name", lit(file_name))
        utils.update_etl_date(
            study_id, dataset_id, file_name, constants.ETLStatus.EXTRACTION_COMPLETED
        )
    except Exception as e:
        print(f"Failed processing {study_id}/{dataset_id}/{file_name}: {e}")
        utils.update_etl_date(
            study_id, dataset_id, constants.ETLStatus.EXTRACTION_FAILED
        )
        raise

    try:
        collection_name = f"study_{study_id}"
        print(f"Saving {study_id}/{dataset_id}/{file_name} --> {collection_name}")
        df.write.format("mongodb").mode("append").option(
            "database", constants.MONGO_DB
        ).option("collection", collection_name).save()
        utils.update_etl_date(
            study_id, dataset_id, file_name, constants.ETLStatus.MONGO_SAVE_COMPLETED
        )
        print(f"Done saving {study_id}/{dataset_id}/{file_name} --> {collection_name}")
        print(
            f"""
        Done processing {study_id}/{dataset_id}/{file_name} --> {collection_name}
        """
        )
    except Exception as e:
        print(
            f"""
        Failed Saving {study_id}/{dataset_id}/{file_name} --> {collection_name}: {e}
        """
        )
        utils.update_etl_date(
            study_id, dataset_id, file_name, constants.ETLStatus.MONGO_SAVE_FAILED
        )
        raise

    try:
        os.remove(file_path_local)
        print(f"File {file_path_local} has been removed after processing.")
    except OSError as e:
        print(f"Error removing file {file_path_local}: {e}")


def process_files_concurrently(files_to_etl):
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        # Adjust max_workers based on the available resources
        future_to_file = {
            executor.submit(
                process_file, f["study_id"], f["dataset_id"], f["file_name"]
            ): (f["study_id"], f["dataset_id"], f["file_name"])
            for f in files_to_etl
        }

        for future in concurrent.futures.as_completed(future_to_file):
            file_info = future_to_file[future]
            try:
                future.result()
            except Exception as exc:
                print(f"{file_info} generated an exception: {exc}")


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

ftp = utils.connect_ftp()
utils.get_files_to_etl()

files_pending = utils.get_pending_extraction_docs()

process_files_concurrently(files_pending)

print("=== ETL Process Complete ===")
