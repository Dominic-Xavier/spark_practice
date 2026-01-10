def read_cities(spark, path):
    """
    Reads city dimension
    """
    return (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(path)
    )

def read_employees(spark, path):
    """
    Reads employee data
    """
    return (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(path)
    )

def read_transactions(spark, path):
    """
    Reads transaction data
    """
    return (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(path)
    )
