FROM python:3.9-slim

WORKDIR /app

COPY utils ./utils

COPY kafka/requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY kafka/consumer.py .

ENTRYPOINT ["python", "consumer.py"]
