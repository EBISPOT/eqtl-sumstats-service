import ftplib
import gzip
import os
import time

from kafka import KafkaProducer
from utils import constants


def list_files(ftp, path):
    ftp.cwd(path)
    files = ftp.nlst()
    print(files)
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
    download_file(ftp, os.path.join(constants.FTP_PATH, file_name), local_file)

    print(f"Reading {local_file}")
    index = 0
    chunk_size = 1024 * 1024
    with gzip.open(local_file, "rt", encoding="utf-8") as f:
        while True:
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
        # value=data,
        value="data",
    )
    producer.flush()


if __name__ == "__main__":
    ftp = connect_ftp()
    files = list_files(ftp, constants.FTP_PATH)

    if not os.path.exists(constants.LOCAL_PATH):
        print(f"Creating {constants.LOCAL_PATH}")
        os.makedirs(constants.LOCAL_PATH)

    for file_info in files:
        print(f"{file_info=}")

        file_name = file_info.split()[-1]
        print(f"{file_name=}")

        if file_name.endswith(".gz"):
            extract_data(ftp, file_name)
            time.sleep(1)

    ftp.quit()
