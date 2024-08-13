import ftplib
import gzip
import json
import os
import time
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
    print(f"Downloading {remote_path} ==> {local_path}")
    with open(local_path, "wb") as f:
        ftp.retrbinary(f"RETR {remote_path}", f.write)


def connect_ftp():
    try:
        print(f"Connecting to {constants.FTP_SERVER}...")
        ftp = ftplib.FTP(constants.FTP_SERVER)
        ftp.login()
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


def extract_data(ftp, file_name, qts_dir):
    print(f"Extracting {file_name=}...")
    local_file = os.path.join(constants.LOCAL_PATH, file_name)
    download_file(ftp, os.path.join(ftp.pwd(), file_name), local_file)

    print(f"Reading {local_file}...")
    index = 0

    for headers, values in read_file_in_chunks(local_file):
        data_dict = dict(zip(headers, values))

        relevant_data = {
            "study_id": qts_dir,
            "molecular_trait_id": data_dict.get("molecular_trait_id"),
            "molecular_trait_object_id": data_dict.get("molecular_trait_object_id"),
            "chromosome": data_dict.get("chromosome"),
            "position": int(data_dict.get("position")),
            "ref": data_dict.get("ref"),
            "alt": data_dict.get("alt"),
            "variant": data_dict.get("variant"),
            "ma_samples": int(data_dict.get("ma_samples")),
            "maf": float(data_dict.get("maf")),
            "pvalue": float(data_dict.get("pvalue")),
            "beta": float(data_dict.get("beta")),
            "se": float(data_dict.get("se")),
            "type": data_dict.get("type"),
            "aan": data_dict.get("aan"),
            "r2": data_dict.get("r2"),
            "gene_id": data_dict.get("gene_id"),
            "median_tpm": float(data_dict.get("median_tpm")),
            "rsid": data_dict.get("rsid"),
        }
        # TODO: remove this debug log
        # print(relevant_data)

        key = f"{file_name}_{index}"
        send_to_kafka(json.dumps(relevant_data), key)

        index += 1

    os.remove(local_file)
    print(f"Data extraction complete for {file_name}.")


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


def update_sync_date(study_id: str, dataset_id: str, status: constants.SyncStatus):
    """
    Updates the sync date, study_id, dataset_id, and status in the MongoDB document.
    """
    current_date = datetime.utcnow()
    collection.update_one(
        {"study_id": study_id, "dataset_id": dataset_id},
        {"$set": {"date": current_date, "status": status.value}},
        upsert=True,
    )


def get_last_sync_date(study_id: str, dataset_id: str):
    """
    Retrieves the last sync date and status for a specific study_id
    and dataset_id from the MongoDB document.
    """
    document = collection.find_one(
        {"study_id": study_id, "dataset_id": dataset_id},
        {"date": 1, "status": 1, "_id": 0},
    )
    if document:
        return document.get("date"), document.get("status")
    else:
        return None, None


if __name__ == "__main__":
    ftp = connect_ftp()

    if not os.path.exists(constants.LOCAL_PATH):
        print(f"Creating local path {constants.LOCAL_PATH}")
        os.makedirs(constants.LOCAL_PATH)

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
                        print(f"{file_info=}")

                        file_name = file_info.split()[-1]
                        print(f"{file_name=}")

                        try:
                            if len(file_info.split()) >= 8:
                                modified_time_str = " ".join(file_info.split()[5:8])
                                if modified_time_str:
                                    modified_time = datetime.strptime(
                                        modified_time_str, "%b %d %Y"
                                    )
                                    print(f"Last modified time is {modified_time}")
                                else:
                                    raise ValueError("Empty modified time string")
                            else:
                                raise ValueError(
                                    "File info does not contain enough parts"
                                )

                            last_sync_date, last_status = get_last_sync_date(
                                qts_dir, qtd_dir
                            )
                            print(
                                f"""
                                For {qts_dir}/{qtd_dir}
                                Last sync date: {last_sync_date}
                                Status: {last_status}
                                """
                            )

                            if file_name.endswith(".gz") and (
                                last_sync_date is None or modified_time > last_sync_date
                            ):
                                update_sync_date(
                                    qts_dir, qtd_dir, constants.SyncStatus.EXTRACTION_IN_PROGRESS
                                )
                                extract_data(ftp, file_name, qts_dir)
                                update_sync_date(
                                    qts_dir, qtd_dir, constants.SyncStatus.EXTRACTION_COMPLETED
                                )
                                print("Sleeping...")
                                time.sleep(60)
                        except ValueError as ve:
                            print(f"Skipping file {file_name} due to error: {ve}")
                            update_sync_date(
                                qts_dir, qtd_dir, constants.SyncStatus.EXTRACTION_FAILED
                            )
                            continue

                    ftp.cwd("..")

    ftp.quit()
