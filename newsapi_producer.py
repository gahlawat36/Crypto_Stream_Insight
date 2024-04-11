from newsapi import NewsApiClient
import json
from kafka import KafkaProducer

# Get your free API key from https://newsapi.org/
key = "4d5234a4742548f0a68af96e5945d091"

# Initialize api endpoint
newsapi = NewsApiClient(api_key=key)

# Define the list of keywords
keywords = ['Bitcoin', 'Ethereum', 'BTC', 'ETH', 'Crypto Currency', 'Crypto', 'Crypto coins']

# Create a query using the 'or' operator
query = " OR ".join(keywords)

# Define the topic
topic = "Crypto_Project"

# Kafka producer configuration
producer = KafkaProducer(bootstrap_servers='localhost:9092')

# Fetch articles using the query
articles = newsapi.get_everything(q=query, language='en')
for article in articles['articles']:
    # Send each article to the Kafka topic
    producer.send(topic, json.dumps(article).encode('utf-8'))
