FROM bitnami/spark:3.1.2

USER root

# Install Python 3 and pip3
RUN install_packages python3 python3-pip

# Install pyspark in Python 3 environment
RUN pip3 install pyspark==3.1.2

WORKDIR /app

# Copy and install Python dependencies
COPY utils ./utils
RUN pip3 install --no-cache-dir -r utils/requirements.txt

# Copy your application code and configuration
COPY spark/spark_app.py .
COPY spark/log4j.properties /opt/bitnami/spark/conf/

# Set the Python interpreter for PySpark
ENV PYSPARK_PYTHON=python3

# Command to run your Spark application
CMD ["/opt/bitnami/spark/bin/spark-submit", \
     "--master", "local[*]", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2,org.mongodb.spark:mongo-spark-connector_2.12:10.3.0", \
     "--conf", "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:/opt/bitnami/spark/conf/log4j.properties", \
     "--conf", "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:/opt/bitnami/spark/conf/log4j.properties", \
     "spark_app.py"]
