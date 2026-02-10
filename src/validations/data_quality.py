from pyspark.sql.functions import datediff, to_date, col, current_date, row_number
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
from pyspark.sql.window import Window
from src.utils.path_utils import resolve_path

def check_nulls(df: DataFrame, columns: list, df_name: str):
    """
    Check for null values in specified columns of a DataFrame.
    Logs the count of nulls found in each column.

    :param df: Input DataFrame to check
    :param columns: List of column names to check for nulls
    :param df_name: Name of the DataFrame (for logging purposes)
    """
    for column in columns:
        null_count = df.filter(col(column).isNull()).count()
        if null_count > 0:
            print(f"Data Quality Check Failed: {null_count} null values found in column '{column}' of DataFrame '{df_name}'")
        else:
            print(f"Data Quality Check Passed: No null values found in column '{column}' of DataFrame '{df_name}'")
    return df.filter(col(column).isNotNull())

def deduplicate(df: DataFrame, subset: list) -> DataFrame:
    """
    Remove duplicate records from a DataFrame based on specified subset of columns.

    :param df: Input DataFrame to deduplicate
    :param subset: List of column names to consider for identifying duplicates
    :return: Deduplicated DataFrame
    """
    window_spec = Window.partitionBy([col(c) for c in subset]).orderBy(col(subset[0]))
    deduplicated_df = df.withColumn("row_num", row_number().over(window_spec)) \
                        .filter(col("row_num") == 1) \
                        .drop("row_num")
    return deduplicated_df