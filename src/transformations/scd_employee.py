import pyspark.sql.functions as F
from pyspark.sql import DataFrame

def apply_scd_type2(existing_df: DataFrame, incoming_df: DataFrame, **kargs) -> DataFrame:
    """
    Applies SCD Type 2 logic for employees
    """

    key, value = kargs.items()

    # Mark existing records as not current
    existing_df = existing_df.withColumn("is_current", F.lit("Y"))

    # Join existing and incoming dataframes

    joined_df = existing_df.join(incoming_df, on="employee_id", how="inner") \
        .withColumn("is_current", F.lit("Y")) \
        .withColumn("end_date", F.current_date())
    
    
    
    # window + effective dates
    return incoming_df
