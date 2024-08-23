import ftplib
import os
import socket
from datetime import datetime
from time import sleep

from pymongo import MongoClient

from utils import constants

# MongoDB connection setup
client = MongoClient(constants.MONGO_URI)
db = client[constants.MONGO_DB]
collection = db[constants.MONGO_COLLECTION_STATUS]


def list_files(ftp, path):
    files = []
    ftp.retrlines(f"LIST {path}", files.append)
    return files


def download_file(remote_path, local_path, retries=3):
    with ftplib.FTP(constants.FTP_SERVER) as ftp:
        ftp.login(user=constants.FTP_USER, passwd=constants.FTP_PASS)

        try:
            # Ensure the directory exists
            local_dir = os.path.dirname(local_path)
            os.makedirs(local_dir, exist_ok=True)

            for attempt in range(retries):
                try:
                    print(
                        f"""
                        Downloading {remote_path}
                        ==> {local_path} (Attempt {attempt + 1})
                        """
                    )
                    with open(local_path, "wb") as f:
                        ftp.retrbinary(f"RETR {remote_path}", f.write, blocksize=1024)
                    print(f"Successfully downloaded {remote_path} ==> {local_path}")
                    break
                except (ftplib.error_perm, socket.error) as e:
                    print(
                        f"""
                        Failed downloading {remote_path}
                        ==> {local_path} (Attempt {attempt + 1}): {e}
                        """
                    )
                    if attempt < retries - 1:
                        sleep(5 * attempt)
                    else:
                        raise
        except Exception as e:
            print(f"Error: {e}")
            raise


def get_pending_extraction_docs():
    """
    Returns all documents from the MongoDB collection where
    the status is ETLStatus.EXTRACTION_PENDING, only retrieving
    the study_id, dataset_id, and file_name fields.
    """
    pending_docs = collection.find(
        {"status": constants.ETLStatus.EXTRACTION_PENDING.value},
        {"study_id": 1, "dataset_id": 1, "file_name": 1, "_id": 0},
    )
    return list(pending_docs)


def update_etl_date(
    study_id: str, dataset_id: str, file_name: str, status: constants.ETLStatus
):
    """
    Updates the etl date, study_id, dataset_id, and status in the MongoDB document.
    """
    current_date = datetime.utcnow()
    collection.update_one(
        {"study_id": study_id, "dataset_id": dataset_id, "file_name": file_name},
        {"$set": {"date": current_date, "status": status.value}},
        upsert=True,
    )


def get_last_etl_date(study_id: str, dataset_id: str, file_name: str):
    """
    Retrieves the last etl date and status for a specific study_id
    and dataset_id from the MongoDB document.
    """
    document = collection.find_one(
        {"study_id": study_id, "dataset_id": dataset_id, "file_name": file_name},
        {"date": 1, "status": 1, "_id": 0},
    )
    return (document.get("date"), document.get("status")) if document else (None, None)


def parse_modified_time(file_info):
    parts = file_info.split()
    if len(parts) < 8:
        raise ValueError("File info does not contain enough parts")

    modified_time_str = " ".join(parts[5:8])
    if not modified_time_str:
        raise ValueError("Empty modified time string")

    if ":" in modified_time_str:
        modified_time_str += f" {datetime.now().year}"
        return datetime.strptime(modified_time_str, "%b %d %H:%M %Y")
    else:
        return datetime.strptime(modified_time_str, "%b %d %Y")


def should_process_file(last_etl_date, modified_time, last_status):
    return (
        last_etl_date is None
        or modified_time > last_etl_date
        or last_status != constants.ETLStatus.EXTRACTION_COMPLETED
    )


def process_file(ftp, qts_dir, qtd_dir, file_info):
    file_name = file_info.split()[-1]
    if not file_name.endswith(".gz"):
        return

    try:
        modified_time = parse_modified_time(file_info)
        last_etl_date, last_status = get_last_etl_date(qts_dir, qtd_dir, file_name)

        if should_process_file(last_etl_date, modified_time, last_status):
            update_etl_date(
                qts_dir, qtd_dir, file_name, constants.ETLStatus.EXTRACTION_PENDING
            )
    except ValueError as ve:
        print(f"Skipping file {file_name} due to error: {ve}")
        update_etl_date(
            qts_dir, qtd_dir, file_name, constants.ETLStatus.EXTRACTION_FAILED
        )


def get_files_to_etl():
    with ftplib.FTP(constants.FTP_SERVER) as ftp:
        ftp.login(user=constants.FTP_USER, passwd=constants.FTP_PASS)
        ftp.cwd(constants.FTP_BASE_PATH)

        for qts_dir_info in list_files(ftp, constants.FTP_BASE_PATH):
            qts_dir = qts_dir_info.split()[-1]
            if not qts_dir.startswith("QTS"):
                continue

            qts_path = os.path.join(constants.FTP_BASE_PATH, qts_dir)
            for qtd_dir_info in list_files(ftp, qts_path):
                qtd_dir = qtd_dir_info.split()[-1]
                if not qtd_dir.startswith("QTD"):
                    continue

                qtd_path = os.path.join(qts_path, qtd_dir)
                ftp.cwd(qtd_path)

                for file_info in list_files(ftp, qtd_path):
                    process_file(ftp, qts_dir, qtd_dir, file_info)

                ftp.cwd("..")
