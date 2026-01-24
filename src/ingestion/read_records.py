def read_records_csv(spark, path):
    """
    Reads records from a CSV file
    """
    return (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(path)
    )

def read_records_json(spark, path):
    """
    Reads records from a JSON file
    """
    return (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .json(path)
    )

def read_records_parquet(spark, path):
    """
    Reads records from a Parquet file
    """
    return (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .parquet(path)
    )

def read_records_delta(spark, path):
    """
    Reads records from a Delta file
    """
    return (
        spark
        .read
        .format("delta")
        .load(path)
    )