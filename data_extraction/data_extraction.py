import ftplib
import gzip
import os
from datetime import datetime

from pymongo import MongoClient

from kafka import KafkaProducer
from utils import constants

# MongoDB connection setup
client = MongoClient(constants.MONGO_URI)
db = client[constants.MONGO_DB]
collection = db[constants.MONGO_COLLECTION_STATUS]


def list_files(ftp, path):
    files = []
    ftp.retrlines(f"LIST {path}", files.append)
    return files


def download_file(ftp, remote_path, local_path):
    try:
        # Ensure the directory exists
        local_dir = os.path.dirname(local_path)
        os.makedirs(local_dir, exist_ok=True)

        print(f"Downloading {remote_path} ==> {local_path}")
        with open(local_path, "wb") as f:
            ftp.retrbinary(f"RETR {remote_path}", f.write)
    except Exception as e:
        print(f"Failed downloading {remote_path} ==> {local_path}: {e}")
        raise


def connect_ftp():
    try:
        print(f"Connecting to {constants.FTP_SERVER}...")
        ftp = ftplib.FTP(constants.FTP_SERVER)
        ftp.login(user=constants.FTP_USER, passwd=constants.FTP_PASS)
        print(f"Connection to {constants.FTP_SERVER} successful")
        return ftp
    except ftplib.error_perm as e:
        print(f"Connection to {constants.FTP_SERVER} failed")
        print(f"FTP error: {e}")
        raise


def read_file_in_chunks(file_path, chunk_size=1024):
    with gzip.open(file_path, "rt", encoding="utf-8") as f:
        headers = f.readline().strip().split("\t")
        while True:
            lines = f.readlines(chunk_size)
            if not lines:
                break
            for line in lines:
                yield headers, line.strip().split("\t")


def send_to_kafka(data, key):
    producer = KafkaProducer(
        bootstrap_servers=constants.BOOTSTRAP_SERVERS,
        api_version=(0, 11, 5),
        key_serializer=str.encode,
        value_serializer=str.encode,
    )
    producer.send(
        constants.KAFKA_TOPIC,
        key=key,
        value=data,
    )
    producer.flush()


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
    if document:
        return document.get("date"), document.get("status")
    else:
        return None, None


def get_files_to_etl() -> None:
    ftp = connect_ftp()

    base_path = constants.FTP_BASE_PATH
    ftp.cwd(base_path)
    qts_dirs = list_files(ftp, base_path)

    for qts_dir_info in qts_dirs:

        qts_dir = qts_dir_info.split()[-1]
        if qts_dir.startswith("QTS"):
            qts_path = os.path.join(base_path, qts_dir)
            qts_files = list_files(ftp, qts_path)

            for qtd_dir_info in qts_files:
                qtd_dir = qtd_dir_info.split()[-1]
                if qtd_dir.startswith("QTD"):
                    qtd_path = os.path.join(qts_path, qtd_dir)
                    ftp.cwd(qtd_path)
                    files = list_files(ftp, qtd_path)

                    for file_info in files:
                        # print(f"{file_info}")

                        file_name = file_info.split()[-1]
                        # print(f"{file_name}")

                        if not file_name.endswith(".gz"):
                            # print('Skipping as file extension is not .gz')
                            continue

                        try:
                            if len(file_info.split()) >= 8:
                                modified_time_str = " ".join(file_info.split()[5:8])
                                if modified_time_str:
                                    if ":" in modified_time_str:
                                        # This indicates that the time is included,
                                        # but the year is missing
                                        # Append the current year to the time string
                                        modified_time_str += f" {datetime.now().year}"
                                        modified_time = datetime.strptime(
                                            modified_time_str, "%b %d %H:%M %Y"
                                        )
                                    else:
                                        modified_time = datetime.strptime(
                                            modified_time_str, "%b %d %Y"
                                        )
                                else:
                                    raise ValueError("Empty modified time string")
                            else:
                                raise ValueError(
                                    "File info does not contain enough parts"
                                )

                            last_etl_date, last_status = get_last_etl_date(
                                qts_dir, qtd_dir, file_name
                            )
                            # print(
                            #     f"""
                            #     For {qts_dir}/{qtd_dir}
                            #     Last etl date: {last_etl_date}
                            #     Status: {last_status}
                            #     """
                            # )

                            if (
                                last_etl_date is None
                                or modified_time > last_etl_date
                                or last_status
                                != constants.ETLStatus.EXTRACTION_COMPLETED
                            ):
                                update_etl_date(
                                    qts_dir,
                                    qtd_dir,
                                    file_name,
                                    constants.ETLStatus.EXTRACTION_PENDING,
                                )
                        except ValueError as ve:
                            print(f"Skipping file {file_name} due to error: {ve}")
                            update_etl_date(
                                qts_dir,
                                qtd_dir,
                                file_name,
                                constants.ETLStatus.EXTRACTION_FAILED,
                            )
                            continue

                    ftp.cwd("..")
    ftp.quit()
