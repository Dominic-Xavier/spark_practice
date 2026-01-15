from pyspark.sql import DataFrame
from utils.enum import WriteMode as mode

def write_partquest(df:DataFrame, mode:mode, path:str, partition_col:str) -> None:
    """
    Write DataFrame to Parquet files partitioned by specified column
    """
    (
        df.write
        .mode(mode.value)
        .partitionBy(partition_col)
        .parquet(path)
    )

def write_csv(df:DataFrame, mode:mode, path:str) -> None:
    """
    Write DataFrame to CSV files
    """
    (
        df.write
        .mode(mode.value)
        .option("header", True)
        .csv(path)
    )

def write_json(df:DataFrame, mode:mode, path:str) -> None:
    """
    Write DataFrame to JSON files
    """
    (
        df.write
        .mode(mode.value)
        .option("header", True)
        .json(path)
    )