import logging
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

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


def city_dim_table(city_df: DataFrame) -> DataFrame: 
    """
    Create city dimension table
    """
    return city_df.dropDuplicates(["city_id"]) \
        .select("*")



def employee_dim_table(employee_df : DataFrame) -> DataFrame:
    """
    Insert employee id based on some logic
    """
    emp_window = Window.orderBy(F.monotonically_increasing_id())

    employee_df_with_id = (
        employee_df
        .withColumn("emp_id", F.row_number().over(emp_window))
    )

    return employee_df_with_id



def sales_fact_table(employee_df : DataFrame, sales_df : DataFrame) -> DataFrame:
    """
    Insert employee id in sales dataframe based on some logic
    """
    employee_count = employee_df.count()

    sales_df_with_emp = (
        sales_df
        .withColumn(
            "emp_id",
            (F.abs(F.hash("trx_id")) % employee_count) + 1
        )
    )

    return sales_df_with_emp

def date_dim_table(sales_df : DataFrame) -> DataFrame:
    """
    Create date dimension table from sales dataframe
    """
    date_dim_df = (
        sales_df
        .withColumn("date", F.to_date("transacted_at"))
        .select("date")
        .dropDuplicates(["date"])
        .withColumn("day", F.dayofmonth("date"))
        .withColumn("month", F.month("date"))
        .withColumn("year", F.year("date"))
        .withColumn("quarter", F.quarter("date"))
    )

    return date_dim_df

def get_transaction_date(sales_df : DataFrame):
    """
    Get transaction date from sales dataframe
    """
    last_processed_date = sales_df.select(F.max(F.to_date("transacted_at")).alias("transaction_date"))
    return last_processed_date.collect()[0]["transaction_date"]


def add_year_month_columns(df: DataFrame, date_col: str) -> DataFrame:
    """
    Add year and month columns to DataFrame based on a date column
    """
    return df.withColumn("year", F.year(F.to_date(F.col(date_col)))) \
        .withColumn("month", F.month(F.to_date(F.col(date_col))))

def prepare_sales_staging(sales_df : DataFrame) -> DataFrame:
    """
    Prepare sales staging table by selecting relevant columns and renaming them
    """
    staged_df = (
        sales_df
        .select("*")
        .withColumn("record_hash", 
            F.sha2(
                F.concat_ws(
                    "||",
                    F.col("retailer_id"),
                    F.col("description"),
                    F.col("amount"),
                    F.col("city_id"),
                    F.col("emp_id")
                ),256
            )
        )
    )

    return staged_df

def scd1_sales_merge(spark: SparkSession, sales_staging_df: DataFrame, sales_fact_df: DataFrame) -> DataFrame: 
    """
    Merge staging and target sales dataframes using SCD Type 1 logic
    """
    sales_staging_df.createOrReplaceTempView("sales_stg")
    sales_fact_df.createOrReplaceTempView("sales_fact")

    final_df = spark.sql("""
        MERGE INTO sales_fact AS tgt
        USING sales_stg AS src
        ON tgt.trx_id = src.trx_id
              
        WHEN MATCHED AND tgt.record_hash != src.record_hash THEN
            UPDATE SET
                tgt.retailer_id = src.retailer_id,
                tgt.description = src.description,
                tgt.amount = src.amount,
                tgt.city_id = src.city_id,
                tgt.emp_id = src.emp_id,
                tgt.record_hash = src.record_hash
        WHEN NOT MATCHED THEN
            INSERT *
        """)
    
    return final_df

def scd1_sales_merge(sales_staging_df: DataFrame, sales_fact_df: DataFrame) -> DataFrame:

    new_records = sales_staging_df.join(
        sales_fact_df,
        on="trx_id",
        how="left_anti")
    
    updated_records = (
        sales_staging_df.alias("src")
        .join(
            sales_fact_df.alias("tgt"),
            on="trx_id",
            how="inner"
        )
        .filter(F.col("src.record_hash") != F.col("tgt.record_hash"))
        .select("src.*")
    )

    unchanged_records = (
        sales_staging_df.alias("src")
        .join(
            sales_fact_df.alias("tgt"),
            on="trx_id",
            how="inner"
        )
        .filter(F.col("src.record_hash") == F.col("tgt.record_hash"))
        .select("src.*")
    )

    final_df = (
        unchanged_records
        .unionByName(updated_records)
        .unionByName(new_records)
    )

    return final_df