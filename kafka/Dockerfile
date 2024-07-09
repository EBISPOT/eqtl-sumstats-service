FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt
RUN pip install confluent-kafka
COPY consumer.py .

ENTRYPOINT ["python", "consumer.py"]
