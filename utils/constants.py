from enum import Enum

KAFKA_TOPIC = "etl_data"
KAFKA_TOPIC_TRANSFORMED = "etl_data_transformed"
KAFKA_SERVER = "localhost:9092"
BOOTSTRAP_SERVERS = "kafka:9092"
GROUP_ID = "mygroup"
OFFSET_RESET = "earliest"
MONGO_URI = "mongodb://mongo:27017"
MONGO_DB = "eqtl_database"
MONGO_COLLECTION = "processed_data"
MONGO_COLLECTION_STATUS = "pipeline_status"
FTP_SERVER = "ftp.ebi.ac.uk"
FTP_USER = "anonymous"
FTP_PASS = ""
LOCAL_PATH = "/tmp/eqtl_files"
FTP_BASE_PATH = "/pub/databases/spot/eQTL/sumstats/"
# FTP_BASE_PATH = "/pub/databases/spot/eQTL/sumstats/QTS000001/QTD000001/"


class SyncStatus(Enum):
    EXTRACTION_PENDING = "pending"
    EXTRACTION_IN_PROGRESS = "in_progress"
    EXTRACTION_COMPLETED = "completed"
    EXTRACTION_FAILED = "failed"
