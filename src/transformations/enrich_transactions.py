from pyspark.sql import DataFrame

def enrich_transactions(sales_df: DataFrame, cities_df: DataFrame) -> DataFrame:
    """
    Enrich transactions with city details
    """
    # Join logic here

    enriched_df = sales_df.join(
        cities_df.alias("c"),
        on=(sales_df.city_id == cities_df.city_id),
        how="inner"
    )

    return enriched_df