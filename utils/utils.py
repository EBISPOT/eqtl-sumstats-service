# import ftplib
# import gzip
# import json
# import os
# import time
# from datetime import datetime

# from pymongo import MongoClient

# from kafka import KafkaProducer
# from utils import constants

# def connect_ftp():
#     try:
#         print(f"Connecting to {constants.FTP_SERVER}...")
#         ftp = ftplib.FTP(constants.FTP_SERVER)
#         ftp.login()
#         print(f"Connection to {constants.FTP_SERVER} successful")
#         return ftp
#     except ftplib.error_perm as e:
#         print(f"Connection to {constants.FTP_SERVER} failed")
#         print(f"FTP error: {e}")
#         raise


# def list_files(ftp, path):
#     files = []
#     ftp.retrlines(f"LIST {path}", files.append)
#     return files

