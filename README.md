
# Crypto Stream Insight

## Introduction
This project implements an ETL (Extract, Transform, Load) pipeline that systematically extracts news articles related to cryptocurrencies from the web, processes this information, and analyzes it for insightful trends. The pipeline utilizes several powerful tools and platforms, including Apache Kafka for message streaming, Apache Spark for data processing, and the Hadoop Distributed File System (HDFS) for storage. This setup is designed to handle real-time data streaming and processing, making it ideal for analyzing the fast-moving world of cryptocurrency news.

The pipeline comprises three main scripts, each serving a distinct purpose within the ETL process:
#### 1. newsapi_producer.py (Producer Script)
This Python script serves as the data extraction component. It uses the NewsAPI to fetch news articles based on predefined cryptocurrency-related keywords. These articles are then serialized into JSON format and published to a Kafka topic, making them available for real-time streaming. This script is essential for initiating the pipeline by supplying raw data from various news sources.
##### Key Functions:
- Initializes connection with NewsAPI using an API key.
- Constructs a query using keywords related to cryptocurrencies like "Bitcoin" and "Ethereum".
- Configures a Kafka producer to send fetched articles to a specific Kafka topic.

#### 2. newsapi_consumer.txt (Consumer Script)
This script, written in Scala for use with Apache Spark, acts as the data processing component. It reads the serialized news articles from the Kafka topic, deserializes them, and processes the data using Spark's powerful distributed computing capabilities. The script performs transformations and aggregations on the data, such as counting the number of articles per source or per author, and then stores the results in Parquet files on HDFS. This component is crucial for turning raw data into structured, queryable information.
##### Key Functions:
- Defines a schema that matches the structure of incoming news articles.
- Reads from the specified Kafka topic and deserializes the JSON data into Spark DataFrames.
- Processes and aggregates the data according to specified queries.
- Writes the processed data to HDFS in Parquet format for efficient storage and analysis.

#### 3. sql_to_csv.sql (Analysis and Export Script)
This script utilizes Spark SQL to perform further analysis on the processed data stored in Parquet files on HDFS. It runs a series of SQL queries to extract meaningful statistics and trends from the data, such as the distribution of articles by publication date or the most prolific authors. The results of these queries are then exported to a CSV file for easy access and sharing. This script is the final step in the pipeline, turning processed data into actionable insights.
##### Key Functions:
- Creates a Spark session with Hive support to enable SQL querying capabilities.
- Defines and executes SQL queries to analyze the data stored in Parquet format.
- Collects the results of these queries and writes them to a CSV file, providing a tangible output from the pipeline.
- Through the coordinated operation of these scripts, the project establishes a robust framework for real-time data ingestion, processing, and analysis. By focusing on cryptocurrency news, it demonstrates the pipeline's ability to deliver timely insights into a rapidly evolving domain.

### Getting Started
These instructions will guide you through setting up and running the project on your local machine for development and testing purposes. You may also choose to make necessary changes to the keywords and source according to your requirements.

### Prerequisites
1.	Apache Kafka and Zookeeper
2.	Apache Spark
3.	Hadoop Distributed File System (HDFS)
4.	Python 3
5.	Scala (for Spark scripts)
6.	NewsAPI key (Get a free API key https://newsapi.org/)

### Installation and Setup
#### Zookeeper and Kafka
1.	Install Zookeeper and Kafka: Follow the official Kafka documentation to download and install Kafka and Zookeeper on your system. Kafka Quickstart : https://kafka.apache.org/quickstart 
2.	Start Zookeeper: Bash command: bin/zookeeper-server-start.sh config/zookeeper.properties
3.	Start Kafka Broker: Bash command: bin/kafka-server-start.sh config/server.properties
#### Apache Spark
Download and install Apache Spark from the official Apache Spark website (https://spark.apache.org/downloads.html ). Ensure you have Java installed on your machine as Spark requires it to run.
Hadoop and HDFS
Set up Hadoop and HDFS on your system following the official Apache Hadoop documentation ( https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/SingleCluster.html )

### Configuration
#### News API Producer
- Edit the Producer Script: In the newsapi_producer.py file, replace the key variable with your NewsAPI key and change the keywords list to match your topic of interest.
- Kafka Topic: Update the topic variable in both producer and consumer scripts to reflect your new topic.
#### Spark Consumer
- Edit the newsapi_consumer.txt (Scala script) to ensure the schema matches the structure of your new topic's data. Update the Kafka topic subscription option to your new topic.

### Running the Pipeline
- Run the Producer: Execute the producer script to start fetching news articles and sending them to Kafka.
“python newsapi_producer.py”
- Run the Consumer: Submit the consumer script to Spark for processing and storing the data in HDFS.
“spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 newsapi_consumer.txt”
- Analysis and Export: Execute the sql_to_csv.sql Spark script to analyze the data and export the results to CSV.
“spark-submit sql_to_csv.sql”

### Customizing the Analysis
1. Edit the source and keywords in the newsapi_producer file according to your requirements 
2. Edit the sql_to_csv.sql file to change or add SQL queries based on your analysis requirements. The provided queries include counting articles by source, author, publication date, and identifying trends.

### Acknowledgments
- NewsAPI for providing an easy-to-use API for fetching news articles.
- Apache Kafka and Spark for their powerful streaming and processing capabilities.
- Hadoop for providing the backbone for storing large datasets.
