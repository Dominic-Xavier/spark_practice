from pyspark.sql import SparkSession
from src.streaming.kafka_reader import read_kafka_stream

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("OrdersStreaming")
    .getOrCreate()
)

raw_stream = read_kafka_stream(
    spark,
    topic="olist_orders",
    bootstrap_servers="localhost:9092"
)

