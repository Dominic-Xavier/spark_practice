from pyspark.sql import DataFrame
import pyspark.sql.functions as F

def total_items(df:DataFrame) -> DataFrame:
    return df.groupBy("order_id") \
        .agg(
            F.count("order_item_id").alias("total_items"),
            F.sum("price").alias("total_price"),
            F.sum("freight_value").alias("total_freight")
        ).withColumn("total_order_value", F.col("total_price") + F.col("total_freight"))

def delivery_days(df:DataFrame) -> DataFrame:
    return df.withColumn("delivery_days", F.date_diff(
            F.col("order_delivered_customer_date"), F.col("order_purchase_timestamp"))
        )