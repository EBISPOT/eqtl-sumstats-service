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
import os
import gzip
import shutil
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
import os
import gzip
import shutil
from pyspark import SparkFiles

def process_file(study_id, dataset_id, df):
    print(f'Start processing files from {study_id}/{dataset_id}')

    # Add study_id and collection_name as new columns
    df = df.withColumn("study_id", lit(study_id))
    df = df.withColumn("collection_name", concat(lit("study_"), col("study_id")))

    # Get distinct collection names
    distinct_collections = df.select("collection_name").distinct().collect()

    # Write data to MongoDB by collection name
    for row in distinct_collections:
        collection_name = row['collection_name']
        write_to_mongo(df, collection_name)

    print(f'Done processing files from {study_id}/{dataset_id}!')


def write_to_mongo(df, collection_name):
    print(f'Writing to collection: {collection_name}')

    # Filter the DataFrame for this particular collection
    # collection_df = df.filter(col("collection_name") == collection_name)
    
    # Write the filtered DataFrame to MongoDB
    # collection_df.write.format("mongo").option("collection", collection_name).mode("append").save()
    df.filter(df.collection_name == collection_name).write.format("mongodb").mode("append").option("database", constants.MONGO_DB).option("collection", collection_name).save()


# Initialize SparkSession
spark = (
    SparkSession.builder.appName("SparkApp")
    .config(
        "spark.mongodb.write.connection.uri",
        f"{constants.MONGO_URI}/{constants.MONGO_DB}.{constants.MONGO_COLLECTION}",
    )
    .getOrCreate()
)

# Define the schema for the JSON data
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

# TODO: fix
# 2024-08-21 10:23:52 2024-08-21 09:23:52 WARN  CSVHeaderChecker:69 - CSV header does not conform to the schema.
# 2024-08-21 10:23:52  Header: molecular_trait_id, chromosome, position, ref, alt, variant, ma_samples, maf, pvalue, beta, se, type, ac, an, r2, molecular_trait_object_id, gene_id, median_tpm
# molecular_trait_id	chromosome	position	ref	alt	variant	ma_samples	maf	pvalue	beta	se	type	ac	an	r2	molecular_trait_object_id	gene_id	median_tpm	rsid
# 2024-08-21 10:23:52  Schema: molecular_trait_id, molecular_trait_object_id, chromosome, position, ref, alt, variant, ma_samples, maf, pvalue, beta, se, type, aan, r2, gene_id, median_tpm, rsid
# 2024-08-21 10:23:52 Expected: molecular_trait_object_id but found: chromosome
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

for (study_id, dataset_id, file_name) in files_to_etl:
    print(f"""filepath: {file_name}---------------------""")

    ############# DOWNLOAD AND READ - ok ################## 
    file_path_remote = f'{constants.FTP_BASE_PATH}{study_id}/{dataset_id}/{file_name}'
    file_path_local = os.path.join(constants.LOCAL_PATH, file_name)
    data_extraction.download_file(ftp, file_path_remote, file_path_local)
    # file_path_tsv_local = file_path_local.replace(".gz", "")
    # with gzip.open(file_path_local, 'rb') as f_in:
    #     with open(file_path_tsv_local, 'wb') as f_out:
    #         shutil.copyfileobj(f_in, f_out)
    df = spark.read.csv(file_path_local, sep="\t", header=True, schema=schema)
    ######################################### 




    ########### READ directly on ftp - not ok ############## 
    # df = spark.read.csv(
    #     f'ftp://{constants.FTP_USER}:{constants.FTP_PASS}@{constants.FTP_SERVER}{constants.FTP_BASE_PATH}{study_id}/{dataset_id}/{file_name}', 
    #     sep="\t", 
    #     header=True, 
    #     schema=schema,
    # )    
    ############################################ 

    ########## RDD - not ok ###############
    # sc = spark.sparkContext
    # rdd = sc.textFile(f'ftp://{constants.FTP_USER}:{constants.FTP_PASS}@{constants.FTP_SERVER}{constants.FTP_BASE_PATH}{study_id}/{dataset_id}/{file_name}')
    ###################################

    ########## DF - not ok ################
    # df = spark.read.text(f'ftp://{constants.FTP_USER}:{constants.FTP_PASS}@{constants.FTP_SERVER}{constants.FTP_BASE_PATH}{study_id}/{dataset_id}/{file_name}')
    ########################################

    ########## addFile - not ok ###############   
    # ftp_path = f'ftp://{constants.FTP_USER}:{constants.FTP_PASS}@{constants.FTP_SERVER}{constants.FTP_BASE_PATH}{study_id}/{dataset_id}/{file_name}'
    # print(ftp_path)
    # spark.sparkContext.addFile(ftp_path)
    # file_path_local = SparkFiles.get(file_name)
    # df = spark.read.csv(file_path_local, sep='\t')
    ###########################################   

    ############################
    df_first_100 = df.limit(10)

    # Show the first few rows of the dataframe
    df_first_100.show()

    # TODO: run this parallel
    process_file(study_id, dataset_id, df_first_100)    

    # TODO: remove temp files
    # CSV file: file:///tmp/eqtl_files/QTD000005.cc.tsv.gz
    try:
        os.remove(file_path_local)
        print(f"File {file_path_local} has been removed after processing.")
    except OSError as e:
        print(f"Error removing file {file_path_local}: {e}")



print("=== ETL Process Complete ===")
