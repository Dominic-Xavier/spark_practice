def read_records_csv(spark, path, _schema=None):
    """
    Reads records from a CSV file
    """
    reader = spark.read.option("header", True)
    
    # Use provided schema if available, otherwise infer
    if _schema is not None:
        reader = reader.schema(_schema)
    else:
        reader = reader.option("inferSchema", True)
    
    return reader.csv(f"file:///{path}")

def read_records_json(spark, path, _schema=None):
    """
    Reads records from a JSON file
    """
    reader = spark.read
    
    # Use provided schema if available, otherwise infer
    if _schema is not None:
        reader = reader.schema(_schema)
    else:
        reader = reader.option("inferSchema", True)
    
    return reader.json(f"file:///{path}")

def read_records_parquet(spark, path, _schema=None):
    """
    Reads records from a Parquet file
    """
    reader = spark.read
    
    # Use provided schema if available, otherwise infer
    if _schema is not None:
        reader = reader.schema(_schema)
    else:
        reader = reader.option("inferSchema", True)
    return reader.parquet(f"file:///{path}")

def read_records_delta(spark, path):
    """
    Reads records from a Delta file
    """
    return (
        spark
        .read
        .option("recursiveFileLookup", "true")
        .format("delta")
        .load(path)
    )