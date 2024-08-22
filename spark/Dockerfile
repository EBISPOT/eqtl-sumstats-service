FROM bitnami/spark:3.1.2

USER root

RUN install_packages python3 python3-pip

WORKDIR /app

COPY utils ./utils
RUN pip install --no-cache-dir -r utils/requirements.txt
COPY spark/spark_app.py .
COPY spark/log4j.properties /opt/bitnami/spark/conf/

CMD ["/opt/bitnami/spark/bin/spark-submit", "--master", "local[*]", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2,org.mongodb.spark:mongo-spark-connector_2.12:10.3.0", "--conf", "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:/opt/bitnami/spark/conf/log4j.properties", "--conf", "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:/opt/bitnami/spark/conf/log4j.properties", "spark_app.py"]
