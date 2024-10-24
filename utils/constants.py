import os
from enum import Enum

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://mongo:27017")
MONGO_DB = os.environ.get("MONGO_DB", "eqtl_database")
MONGO_COLLECTION = os.environ.get("MONGO_COLLECTION", "processed_data")
MONGO_COLLECTION_STATUS = os.environ.get("MONGO_COLLECTION_STATUS", "pipeline_status")
FTP_SERVER = "ftp.ebi.ac.uk"
HTTP_SERVER = "http://ftp.ebi.ac.uk"
FTP_USER = "anonymous"
FTP_PASS = "ftppass"
LOCAL_PATH = "/tmp/eqtl_files"
SCRATCH_PATH = "/hps/nobackup/parkinso/spot/gwas/scratch/eqtl/"
FTP_BASE_PATH = "/pub/databases/spot/eQTL/sumstats/"
ENV = os.environ.get("ENV", "local")

class ETLStatus(Enum):
    EXTRACTION_PENDING = "EXTRACTION_PENDING"
    DOWNLOAD_IN_PROGRESS = "DOWNLOAD_IN_PROGRESS"
    DOWNLOAD_COMPLETED = "DOWNLOAD_COMPLETED"
    DOWNLOAD_FAILED = "DOWNLOAD_FAILED"
    EXTRACTION_IN_PROGRESS = "EXTRACTION_IN_PROGRESS"
    EXTRACTION_FAILED = "EXTRACTION_FAILED"
    MONGO_SAVE_COMPLETED = "MONGO_SAVE_COMPLETED"
    MONGO_SAVE_FAILED = "MONGO_SAVE_FAILED"
    EXTRACTION_COMPLETED = "EXTRACTION_COMPLETED"
