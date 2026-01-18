from pyspark.sql import DataFrame
from pyspark.sql.window import Window
import pyspark.sql.functions as F

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