import os
from pyspark.sql import DataFrame
from utils.enum import WriteMode as mode

def write_partquest(df:DataFrame, mode:mode, path:str, partition_col=None) -> None:
    """
    Write DataFrame to Parquet files partitioned by specified column
    """
    directory = os.path.dirname(path)

    if directory and not os.path.exists(directory):
        os.makedirs(directory, exist_ok=True)

    writer = df.write.mode(mode.value)

    if partition_col:
        writer = writer \
        .partitionBy(partition_col)

    writer.parquet(path)

def write_csv(df:DataFrame, mode:mode, path:str, partition_col=None) -> None:
    """
    Write DataFrame to CSV files
    """
    (
        df.write
        .mode(mode.value)
        .option("header", True)
        .partitionBy(partition_col)
        .csv(path)
    )

def write_json(df:DataFrame, mode:mode, path:str, partition_col=None) -> None:
    """
    Write DataFrame to JSON files
    """
    (
        df.write
        .mode(mode.value)
        .option("header", True)
        .partitionBy(partition_col)
        .json(path)
    )