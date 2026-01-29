from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

def get_spark(app_name: str = "DeltaTest", host: str = "local[*]") -> SparkSession:
    """
    Create and return a SparkSession with specific configurations.

    Returns:
        SparkSession: Configured SparkSession object.
    """

    builder = (
        SparkSession.builder
        .appName(app_name)
        .master(host)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    print("Spark Version:", spark.version)

    return spark