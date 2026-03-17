from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F
from delta.tables import DeltaTable

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
                    F.xxhash64(
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
                        F.col("order_item_id"),
                        F.col("order_purchase_timestamp")
                    )
                )
            )
    return staged_df

def upsert(spark: SparkSession, source:DataFrame, target_path:str):
    source.createOrReplaceTempView("source")
    spark.sql(f"""
        MERGE INTO {target_path} t
        USING source s
        ON t.order_id = s.order_id
        AND t.order_item_id = s.order_item_id

        WHEN MATCHED AND t.record_hash != s.record_hash THEN
            UPDATE SET *
        WHEN NOT MATCHED THEN
            INSERT *
    """)

def upsert_pyspark(spark: SparkSession, source: DataFrame, target_path: str):

    delta_table = DeltaTable.forPath(spark, target_path)
    (
        delta_table.alias("t")
        .merge(
            source.alias("s"),
            "t.customer_id = s.customer_id"
        )
        .whenMatchedUpdate(
            condition="t.record_hash != s.record_hash"
        )
        .whenNotMatchedInsertAll()
        .execute()
    )