# Big Data Project: Real-time News Sentiment Analysis and Zero-Shot Classification

## Overview

This project implements a real-time news sentiment analysis and zero-shot classification pipeline using Apache Airflow, Kafka, Spark, and Hugging Face's Transformers. It automates the process of ingesting news data, transforming and cleaning it, performing sentiment analysis, and categorizing news articles using zero-shot classification.

## Architecture

The pipeline consists of the following components:

1.  **Data Source**: CSV File (`data.csv`) containing the news data.
2.  **File Sensor (Airflow):** Monitors the presence of the `data.csv` file, triggering the subsequent tasks upon detection.
3.  **Producer (Python/Kafka):** Reads the `data.csv` file and publishes the news data to a Kafka topic.
4.  **Consumer (Python/Kafka):** Subscribes to the Kafka topic and consumes the news data.
5.  **Data Cleaning (PySpark):** Cleans and transforms the consumed data using Apache Spark.
6.  **News Sentiment Analysis (PySpark):** Performs sentiment analysis on the cleaned news text using NLTK's VADER lexicon.
7.  **Zero-Shot Classification (Python/Transformers):** Categorizes news articles using a pre-trained zero-shot classification model from Hugging Face's Transformers library.
8.  **Workflow Orchestration (Airflow):** Manages and schedules the entire pipeline using Apache Airflow.

## Components

### 1. Airflow DAG (`kafka_spark_dag.py`)

The Airflow DAG (`kafka_spark_dag.py`) defines the workflow, including the following tasks:

*   **`wait_for_file`**: `FileSensor` task that waits for the `data.csv` file to be available.
*   **`producer`**: `BashOperator` task that executes the `producer.py` script to publish data to Kafka.
*   **`consumer`**: `BashOperator` task that executes the `consumer.py` script to consume data from Kafka.
*   **`data_cleaning`**: `BashOperator` task that executes the `clean_in_spark.py` script to clean data using Spark.
*   **`news_sentiment`**: `BashOperator` task that executes the `news_sentiment_in_hdfs.py` script to perform sentiment analysis using Spark.
*   **`zeroShot_classification`**: `BashOperator` task that executes the `zeroShot_classificatioN.py` script to perform zero-shot classification.

The DAG defines the dependencies between the tasks, ensuring they run in the correct order.

### 2. Data Producer (`producer.py`)

The `producer.py` script reads news data from the input CSV file (`data.csv`) and publishes it to a Kafka topic.  It leverages the Kafka Python client library.

### 3. Data Consumer (`consumer.py`)

The `consumer.py` script subscribes to the Kafka topic, consumes the messages (news data), and performs basic processing before passing it to the data cleaning stage. It leverages the Kafka Python client library.

### 4. Data Cleaning (`clean_in_spark.py`)

The `clean_in_spark.py` script uses Apache Spark to perform data cleaning and transformation tasks. This includes:

*   Removing duplicates
*   Handling missing values
*   Text cleaning (lowercase, punctuation removal)

### 5. News Sentiment Analysis (`news_sentiment_in_hdfs.py`)

The `news_sentiment_in_hdfs.py` script utilizes Apache Spark and NLTK's VADER lexicon to perform sentiment analysis on the news data. It calculates sentiment scores for each news article and stores the results in HDFS (Hadoop Distributed File System).

### 6. Zero-Shot Classification (`zeroShot_classificatioN.py`)

The `zeroShot_classificatioN.py` script uses Hugging Face's Transformers library to classify news articles into predefined categories without requiring explicit training data for those categories. This enables flexible and adaptable categorization of news content.

## Dependencies

*   **Apache Airflow:** For workflow orchestration.
*   **Apache Kafka:** For real-time data streaming.
*   **Apache Spark:** For data processing and analysis.
*   **Python 3.x:** For running the producer, consumer, data cleaning, sentiment analysis, and zero-shot classification scripts.
*   **NLTK (Natural Language Toolkit):** For sentiment analysis (VADER lexicon).
*   **Hugging Face Transformers:** For zero-shot classification.
*   **Required Python Packages:**  A `requirements.txt` file should be included in your repository containing all necessary Python packages (including `kafka-python`, `pyspark`, `nltk`, `transformers`, `torch`).  Install using `pip install -r requirements.txt`.

## Installation

1.  **Clone the repository:**
    ```
    git clone <repository_url>
    cd <repository_directory>
    ```

2.  **Install dependencies:**
    ```
    pip install -r requirements.txt
    ```

3.  **Configure Apache Airflow:**
    *   Install Airflow: Follow the official Airflow documentation for installation instructions.
    *   Update `airflow.cfg` with correct configurations, like `executor = SequentialExecutor` (for testing) or a more scalable executor.

4.  **Configure Apache Kafka:**
    *   Install and start ZooKeeper and Kafka.
    *   Create a Kafka topic to which the producer will publish data.
    *   Update the `producer.py` and `consumer.py` scripts with the correct Kafka broker address and topic name.

5.  **Configure Apache Spark:**
    *   Install Apache Spark.
    *   Ensure that Spark is correctly configured to run in your environment (local, standalone, or cluster).

6.  **Hugging Face Transformers:**
    * Make sure `torch` is installed, as it's a dependency for the transformers library. The `requirements.txt` should handle this. Also, the first time you run the zero-shot classification script, it will download the necessary model, which can take some time.

## Usage

1.  Place the `data.csv` file in the specified `filepath` directory `/home/atul/Desktop/project_big_data/data.csv` (or update the `filepath` in the `kafka_spark_dag.py` file).
2.  Start the Airflow scheduler: `airflow scheduler`
3.  Start the Airflow webserver: `airflow webserver`
4.  Enable the `kafka_spark_dag` DAG in the Airflow UI.
5.  The DAG will automatically run according to the defined `schedule_interval`.

## Future Enhancements

*   **Dynamic Input Path:** Make the input file path configurable via Airflow variables.
*   **Scalability:** Implement Spark on a cluster to handle large datasets more efficiently.
*   **Data Validation:** Add data validation steps to ensure data quality.
*   **Monitoring:** Implement monitoring to track the performance of the pipeline.
*   **Implement other Machine Learning techniques:** Such as clustering or predictive analytics using the sentiment scores and classified data.
*   **Refine Zero-Shot Classification:** Experiment with different pre-trained models and candidate labels to improve the accuracy of zero-shot classification.
*   **Implement a Data Lake/Warehouse:** Store the processed data in a data lake (e.g., using Apache Hadoop or AWS S3) or a data warehouse (e.g., using Apache Hive or Snowflake) for long-term storage and analysis.
*   **Implement REST API:** To access and visualize the data.

## Author

Atul
