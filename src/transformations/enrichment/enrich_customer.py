from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

def dim_customer_order_metrics_sql(spark: SparkSession, df: DataFrame)->DataFrame:
    df.createOrReplaceTempView("customer_dim_table")
    return spark.sql('''
                select
                    customer_id,
                    count(distinct order_id) as total_orders,
                    round(sum(total_order_value),2) as total_spent,
                    round(avg(total_order_value),2) as avg_order_value,
                    min(order_purchase_timestamp) as first_order_date,
                    max(order_purchase_timestamp) as last_order_date,
                    round(avg(cast(review_score as double)),2) as avg_review_score
                from customer_dim_table
                group by customer_id
              ''')

def dim_customer_order_metrics(df: DataFrame)->DataFrame:
    df.groupBy("customer_unique_id")\
    .agg(
        F.count_distinct("order_id").alias("total_orders"),
        F.round(
            F.sum("total_order_value"),2
        ).alias("total_spent")
    )