from pyspark.sql import SparkSession

def get_spark(app_name: str, host: str = "local[*]") -> SparkSession:
    """
    Create and return a SparkSession with specific configurations.

    Returns:
        SparkSession: Configured SparkSession object.
    """
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master(host)
        .getOrCreate()
    )
    return spark