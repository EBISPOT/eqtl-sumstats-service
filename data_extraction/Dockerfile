FROM python:3.9-slim

WORKDIR /app

COPY utils ./utils

COPY data_extraction/requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY data_extraction/data_extraction.py .

ENTRYPOINT ["python", "data_extraction.py"]
