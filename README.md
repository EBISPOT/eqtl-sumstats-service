# eQTL Summary Statistics Service

This project implements an ETL pipeline for extracting, transforming, and loading eQTL summary statistics data into a MongoDB database. The pipeline is built using Python and integrates various technologies like Apache Kafka, Apache Spark, and MongoDB.

## Project Structure

```
EQTL-SUMSTATS-SERVICE/
│
├── data_extraction/
│   ├── __init__.py
│   ├── data_extraction.py
│   ├── Dockerfile
│   └── requirements.txt
├── env_formatlint/
├── kafka/
│   ├── __init__.py
│   ├── data_transformation.py
│   ├── Dockerfile
│   └── requirements.txt
├── mongo/
├── spark/
│   ├── __init__.py
│   ├── Dockerfile
│   ├── log4j.properties
│   └── spark_app.py
├── utils/
│   ├── __init__.py
│   └── constants.py
├── .gitignore
├── docker-compose.yml
├── format-lint
├── pytype.cfg
├── README.md
└── requirements.dev.txt
```

## Prerequisites

- Docker
- Docker Compose
- Python 3.8 or higher
- Java 8 or higher (for Apache Spark)

## Installation

1. **Clone the repository**:

   ```bash
   git clone https://github.com/yourusername/eqtl-sumstats-service.git
   cd eqtl-sumstats-service
   ```

2. **Build Docker images**:
   ```bash
   docker-compose build
   ```

## Configuration

Configure the necessary environment variables and settings in the `.env` file or directly in the `docker-compose.yml` file.

## Usage

### Running the Pipeline

1. **Start the services**:

   ```bash
   docker-compose up
   ```

2. **Data Extraction**:
   The `data_extraction` service downloads and extracts data from the specified FTP server.

3. **Data Transformation**:
   The `kafka` service reads data from Kafka topics, processes it using Spark, and prepares it for loading into MongoDB.

4. **Data Loading**:
   The transformed data is loaded into MongoDB for storage and querying.

### Components

#### Data Extraction

Located in the `data_extraction` directory. This component handles downloading and extracting eQTL data from an FTP server.

- **data_extraction.py**: Main script for data extraction.
- **Dockerfile**: Docker configuration for the data extraction service.
- **requirements.txt**: Python dependencies for data extraction.

#### Kafka

Located in the `kafka` directory. This component handles data transformation using Apache Kafka and Spark.

- **data_transformation.py**: Main script for data transformation.
- **Dockerfile**: Docker configuration for the Kafka service.
- **requirements.txt**: Python dependencies for Kafka and Spark.

#### Spark

Located in the `spark` directory. This component handles processing of data using Apache Spark.

- **spark_app.py**: Main script for Spark data processing.
- **Dockerfile**: Docker configuration for the Spark service.
- **log4j.properties**: Logging configuration for Spark.

#### Utils

Located in the `utils` directory. Contains utility functions and constants used across the project.

- **constants.py**: Defines constant values used in the project.

## Misc.

### Format & Lint

Create a virtual env for format & lint in which you can install the required Python packages using:

```bash
pip install -r requirements.dev.txt
```

One can run the script as follows:

```bash
./format-lint
```
