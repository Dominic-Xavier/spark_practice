from pyspark.sql import SparkSession

def get_spark(app_name: str, host: str = "local[*]") -> SparkSession:
    '''
    Create and return a SparkSession with specific configurations.

    Returns:
        SparkSession: Configured SparkSession object.
    '''
    

    builder = (
        SparkSession.builder
        .appName("DeltaTest")
        .master("local[*]")
        .config("spark.hadoop.fs.defaultFS", "file:///")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.shuffle.targetPostShuffleInputSize", "128MB")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.autoBroadcastJoinThreshold", "-1")
        .config("spark.sql.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", 128 * 1024 * 1024)
    )

    spark = builder.getOrCreate()

    print("Spark Version:", spark.version)

    return spark