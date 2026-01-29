from pyspark.sql import SparkSession

def read_kafka_stream(
    spark: SparkSession,
    topic: str,
    bootstrap_servers: str,
    starting_offsets: str = "latest"
):
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("maxOffsetsPerTrigger", "100")
        .option("startingOffsets", starting_offsets)
        .load()
    )