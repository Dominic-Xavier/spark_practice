from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F

def total_items(df:DataFrame) -> DataFrame:
    return df.groupBy("order_id") \
        .agg(
            F.count("order_item_id").alias("total_items"),
            F.sum("price").alias("total_price"),
            F.sum("freight_value").alias("total_freight")
        ).withColumn("total_order_value", F.round(F.col("total_price") + F.col("total_freight"),2))

def delivery_days(df:DataFrame) -> DataFrame:
    return df.withColumn("delivery_days", F.date_diff(
            F.col("order_delivered_customer_date"), F.col("order_purchase_timestamp"))
        )

def prepare_Fact_360_staging(source_df:DataFrame):
    staged_df = (
            source_df
                 .select("*")
                 .withColumn("record_hash",
                    F.sha2(
                        F.concat_ws(
                            "||",
                            F.col("customer_id"),
                            F.col("order_id"),
                            F.col("customer_city"),
                            F.col("customer_state"),
                            F.col("seller_id"),
                            F.col("product_id"),
                            F.col("total_items"),
                            F.col("total_order_value"),
                            F.col("payment_type"),
                            F.col("review_score"),
                            F.col("delivery_days"),
                            F.col("order_purchase_timestamp")
                        ),256
                    )
                )
            )
    return staged_df

def upsert(spark: SparkSession, source:DataFrame, target:DataFrame):
    target.createOrReplaceTempView("target")
    source.createOrReplaceTempView("source")
    final_df = spark.sql("""
                Merge  into source as s
                using target as t
                on s.customer_id = t.customer_id
                when matched AND t.record_hash != s.record_hash THEN 
                    update set
                        t.customer_id = s.customer_id,
                        t.order_id = s.order_id,
                        t.customer_city = s.customer_city,
                        t.customer_state = s.customer_state
                        t.seller_id = s.seller_id
                        t.product_id = s.product_id
                        t.total_items = s.total_items
                        t.total_order_value = s.total_order_value
                        t.payment_type = s.payment_type
                        t.review_score = s.review_score
                        t.delivery_days = s.delivery_days
                        t.order_purchase_timestamp = s.order_purchase_timestamp
                when not matched
                    insert *
            """)
    return final_df