import os
from pyspark.sql import DataFrame
from src.utils.enum import WriteMode as mode

def write_partquet(df:DataFrame, mode:mode, path:str, *partition_col) -> None:
    """
    Write DataFrame to Parquet files partitioned by specified column
    """
    #directory = os.path.dirname(path)

    """if directory and not os.path.exists(directory):
        os.makedirs(directory, exist_ok=True)"""

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

def write_parquet_delta(df: DataFrame, mode, path: str, *partitions, table_name: str = None):

    if not path:
        raise ValueError("Target path cannot be empty")

    writer = df.write.format("delta").mode(mode.value)

    if partitions:
        writer = writer.partitionBy(*partitions)

    if table_name:
        writer.option("path", path).saveAsTable(table_name)
    else:
        writer.save(path)