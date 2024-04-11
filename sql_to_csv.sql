from pyspark.sql import SparkSession

# Create a Spark session with Hive support
spark = SparkSession.builder \
    .appName("Crypto News Articles Analysis") \
    .enableHiveSupport() \
    .getOrCreate()

# Define the queries and descriptions
queries = [
    {
        "description": "Total Number of Articles:",
        "query": "SELECT COUNT(*) AS result FROM crypto_new_articles"
    },
    {
        "description": "Number of Unique Authors:",
        "query": "SELECT COUNT(DISTINCT author) AS result FROM crypto_new_articles"
    },
    {
        "description": "Number of Unique Sources:",
        "query": "SELECT COUNT(DISTINCT source.name) AS result FROM crypto_new_articles"
    },
    {
        "description": "Earliest Publication Date:",
        "query": "SELECT MIN(publishedAt) AS result FROM crypto_new_articles"
    },
    {
        "description": "Latest Publication Date:",
        "query": "SELECT MAX(publishedAt) AS result FROM crypto_new_articles"
    },
    {
        "description": "Articles by Source",
        "query": "SELECT source.name AS source_name, COUNT(*) AS number_of_articles FROM crypto_new_articles GROUP BY source.name ORDER BY number_of_articles DESC"
    },
    {
        "description": "Top 5 Sources by Article Count",
        "query": "SELECT source.name AS source_name, COUNT(*) AS number_of_articles FROM crypto_new_articles GROUP BY source.name ORDER BY number_of_articles DESC LIMIT 5"
    },
    {
        "description": "Top 10 Authors by Article Count",
        "query": "SELECT author, COUNT(*) AS number_of_articles FROM crypto_new_articles WHERE author IS NOT NULL GROUP BY author ORDER BY number_of_articles DESC LIMIT 10"
    },
    {
        "description": "Articles Count by Publication Date",
        "query": "SELECT TO_DATE(CAST(from_unixtime(unix_timestamp(publishedAt, \"yyyy-MM-dd'T'HH:mm:ss'Z'\")) AS TIMESTAMP)) AS publication_date, COUNT(*) AS number_of_articles FROM crypto_new_articles GROUP BY TO_DATE(CAST(from_unixtime(unix_timestamp(publishedAt, \"yyyy-MM-dd'T'HH:mm:ss'Z'\")) AS TIMESTAMP)) ORDER BY publication_date DESC"
    },
    {
        "description": "Monthly Article Count Trend",
        "query": "SELECT YEAR(CAST(from_unixtime(unix_timestamp(publishedAt, \"yyyy-MM-dd'T'HH:mm:ss'Z'\")) AS TIMESTAMP)) AS year, MONTH(CAST(from_unixtime(unix_timestamp(publishedAt, \"yyyy-MM-dd'T'HH:mm:ss'Z'\")) AS TIMESTAMP)) AS month, COUNT(*) AS number_of_articles FROM crypto_new_articles GROUP BY YEAR(CAST(from_unixtime(unix_timestamp(publishedAt, \"yyyy-MM-dd'T'HH:mm:ss'Z'\")) AS TIMESTAMP)), MONTH(CAST(from_unixtime(unix_timestamp(publishedAt, \"yyyy-MM-dd'T'HH:mm:ss'Z'\")) AS TIMESTAMP)) ORDER BY year DESC, month DESC"
    }
]

# Open a file to write the results
with open('/home/pradeepgahlawat36/crypto_analysis_results.csv', 'w') as file:
    # Iterate through the queries and write the results
    for item in queries:
        description = item["description"]
        query = item["query"]
        results = spark.sql(query).collect()
        file.write(f"{description}\n")
        for result in results:
            file.write(",".join([str(value) for value in result]) + "\n")
        file.write("\n")

# Stop the Spark session
spark.stop()
