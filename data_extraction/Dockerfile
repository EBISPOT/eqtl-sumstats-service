FROM python:3.9-slim

WORKDIR /app

COPY utils ./utils

RUN pip install --no-cache-dir -r utils/requirements.txt

ENTRYPOINT ["python", "data_extraction.py"]
