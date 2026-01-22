import pyspark.sql.functions as F
from pyspark.sql import DataFrame

def prepare_employee_staging(employee_df) -> DataFrame:
    """
    Prepare employee staging table by selecting relevant columns and renaming them
    """
    staging_df = (
        employee_df
        .select("*")
        .withColumn("record_hash", 
            F.sha2(F.concat_ws("||",
            F.col("first_name"),
            F.col("last_name"),
            F.col("job_title"),
            F.col("phone"),
            F.col("salary"),
            F.col("department_id")
        ), 256))
    .withColumn("effective_start_date", F.current_date())
    .withColumn("effective_end_date", F.lit(None).cast("date"))
    .withColumn("is_current", F.lit(True)))

    return staging_df

def scd2_employee_merge(staging_df, target_df) -> DataFrame:
    """
    Merge staging and target employee dataframes using SCD Type 2 logic
    """
    # Implementation of SCD Type 2 merge logic here
    current_target = target_df.filter(F.col("is_current") == True)

    joined_df = staging_df.alias("src").join(current_target.alias("tgt"), on="emp_id", how="left")

    new_records = joined_df.filter(F.col("tgt.emp_id").isNull() & (F.col("src.record_hash") != F.col("tgt.record_hash"))) \
        .select("src.*")
    
    # 2️⃣ CHANGED employees
    changed_records = joined_df.filter(
        (F.col("tgt.emp_id").isNotNull()) &
        (F.col("src.record_hash") != F.col("tgt.record_hash"))
    )

    expired_records = changed_records.withColumn(F.col("tgt.effective_end_date"), F.current_date()) \
        .withColumn(F.col("tgt.is_current"), F.lit(False))
    
    new_version = changed_records.select("src.*")

    #Final Dataframe
    final_df = (
        target_df.filter(F.col("is_current") == False)
        .unionByName(expired_records.select("tgt.*"))
        .unionByName(new_records)
        .unionByName(new_version)
    )

    return final_df