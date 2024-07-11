FROM bitnami/spark:3.1.2

USER root

# Install necessary packages
RUN install_packages python3 python3-pip

WORKDIR /app

COPY spark_app.py spark_app.py

ENV SPARK_JARS_PACKAGES org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2

CMD ["/opt/bitnami/spark/bin/spark-submit", "--master", "local[*]", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2", "spark_app.py"]
