import ftplib
import gzip
import os
import time
from datetime import datetime

from kafka import KafkaProducer
from utils import constants


def list_files(ftp, path):
    files = []
    ftp.retrlines(f"LIST {path}", files.append)
    # print("== LISTING FILES ==")
    # print(files)
    return files


def download_file(ftp, remote_path, local_path):
    print(f"Downloading {remote_path} --> {local_path}")
    with open(local_path, "wb") as f:
        ftp.retrbinary(f"RETR {remote_path}", f.write)


def connect_ftp():
    try:
        ftp = ftplib.FTP(constants.FTP_SERVER)
        ftp.login()
        return ftp
    except ftplib.error_perm as e:
        print(f"FTP error: {e}")
        raise


def extract_data(ftp, file_name):
    print(f"Extracting {file_name=}")
    local_file = os.path.join(constants.LOCAL_PATH, file_name)
    download_file(ftp, os.path.join(ftp.pwd(), file_name), local_file)

    print(f"Reading {local_file}")
    index = 0
    chunk_size = 1024 * 1024
    with gzip.open(local_file, "rt", encoding="utf-8") as f:
        # TODO: Replace
        while True:
            # while True and index < 2:
            data = f.read(chunk_size)
            if not data:
                break
            key = f"{file_name}_{index}"
            print(f"Sending {key} to Kafka")
            send_to_kafka(data, key)

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
        # TODO: Use data in real case
        value=data,
        # value="data",
    )
    producer.flush()


def update_sync_date(sync_log_path, sync_date):
    with open(sync_log_path, "w") as f:
        f.write(sync_date.strftime("%Y-%m-%d %H:%M:%S"))


def get_last_sync_date(sync_log_path):
    if os.path.exists(sync_log_path):
        with open(sync_log_path, "r") as f:
            last_sync = f.read().strip()
        return datetime.strptime(last_sync, "%Y-%m-%d %H:%M:%S")
    return None


if __name__ == "__main__":
    ftp = connect_ftp()

    last_sync_date = get_last_sync_date(constants.SYNC_LOG_PATH)

    if not os.path.exists(constants.LOCAL_PATH):
        print(f"Creating {constants.LOCAL_PATH}")
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
                            # Ensure file_info has enough parts
                            if len(file_info.split()) >= 8:
                                modified_time_str = " ".join(file_info.split()[5:8])
                                if modified_time_str:
                                    modified_time = datetime.strptime(
                                        modified_time_str, "%b %d %Y"
                                    )
                                else:
                                    raise ValueError("Empty modified time string")
                            else:
                                raise ValueError(
                                    "File info does not contain enough parts"
                                )

                            # Proceed if the file is new or updated since the last sync
                            if file_name.endswith(".gz") and (
                                last_sync_date is None or modified_time > last_sync_date
                            ):
                                extract_data(ftp, file_name)
                                time.sleep(1)
                        except ValueError as ve:
                            print(f"Skipping file {file_name} due to error: {ve}")
                            continue

                    ftp.cwd("..")

    update_sync_date(constants.SYNC_LOG_PATH, datetime.now())
    ftp.quit()
