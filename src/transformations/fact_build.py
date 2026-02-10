

from pyspark.sql import DataFrame
from pyspark.sql.window import Window
import pyspark.sql.functions as F


def join_df(sales_df: DataFrame, cities_df: DataFrame) -> DataFrame:
    """Join two dataframes to enrich data

    Args:
        sales_df (DataFrame): Sales DataFrame
        cities_df (DataFrame): Cities DataFrame

    Returns:
        DataFrame: Enriched DataFrame
    """

    