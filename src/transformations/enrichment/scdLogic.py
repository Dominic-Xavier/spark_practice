from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from delta.tables import DeltaTable

def customer_staging(staging_df:DataFrame):
    return staging_df.withColumns({
        "record_hash":F.xxhash64("customer_zip_code_prefix", "customer_city", "customer_state"),
        "isCurrent":F.lit("Y"),
        "effective_start_date":F.current_date(),
        "effective_end_date":F.lit("null")
    })

def product_staging(staging_df:DataFrame):
    return staging_df.withColumns({
        "recorsd_hash":F.xxhash64("product_category_name", "product_weight_g", 
            "product_length_cm", "product_height_cm", "product_width_cm"),
        "isCurrent":F.lit("Y"),
        "effective_start_date":F.current_date(),
        "effective_end_date":F.lit("null")
    })

def seller_staging(staging_df:DataFrame):
    return staging_df.withColumns({
        "record_hash":F.xxhash64("seller_zip_code_prefix", "seller_city", "seller_state"),
        "isCurrent":F.lit("Y"),
        "effective_start_date":F.current_date(),
        "effective_end_date":F.lit("null")
    })

def scd_type2_merge_customer(spark: SparkSession, source_df:DataFrame, target_path:str):
    delta_table = DeltaTable = DeltaTable.forPath(spark, target_path)
    (
        delta_table.alias("t")
        .merge(
            source_df.alias("s"),
            "t.customer_id = s.customer_id AND t.is_current = Y"
        )
        .whenMatchedUpdate(
            condition = "t.record_hash != s.record_hash",
            set={
                "effective_end_date": "current_timestamp()",
                "is_current": "N"
            }
        )
        .whenNotMatchedInsert(
            values={
                "customer_id": "s.customer_id",
                "customer_unique_id":"s.customer_unique_id",
                "customer_city": "s.customer_city",
                "customer_zip_code_prefix": "s.customer_zip_code_prefix",
                "customer_city": "s.customer_city",
                "customer_state": "s.customer_state",
                "record_hash": "s.record_hash",
                "effective_start_date": "current_timestamp()",
                "effective_end_date": "null",
                "is_current": "Y"
            }
        )
        .execute()
    )

def scd_type2_merge_customer_sql(spark: SparkSession, source_df:DataFrame, target_path:str):
    source_df.createOrReplaceTempView("source_view")

    spark.sql(f'''
        MERGE INTO delta.`{target_path}` t
        USING source_view s
        ON t.customer_id = s.customer_id AND t.is_current = 'Y'

        WHEN MATCHED AND t.record_hash != s.record_hash THEN
        UPDATE SET
            t.effective_end_date = current_timestamp(),
            t.is_current = 'N'

        WHEN NOT MATCHED THEN
        INSERT (
            customer_id,
            customer_zip_code_prefix,
            customer_city,
            customer_state,
            record_hash,
            effective_start_date,
            effective_end_date,
            is_current
        )
        VALUES (
            s.customer_id,
            s.customer_zip_code_prefix,
            s.customer_city,
            s.customer_state,
            s.record_hash,
            current_timestamp(),
            NULL,
            'Y'
        )

    ''')

def scd_type2_merge_Product(spark: SparkSession, source_df:DataFrame, target_path:str):
    delta_table = DeltaTable = DeltaTable.forPath(spark, target_path)
    (
        delta_table.alias("t")
        .merge(
            source_df.alias("s"),
            "t.product_id = s.product_id AND t.is_current = 'Y'"
        )
        .whenMatchedUpdate(
            condition = "t.record_hash != s.record_hash",
            set={
                "effective_end_date": "current_timestamp()",
                "is_current": "N"
            }
        )
        .whenNotMatchedInsert(
            values={
                "product_id": "s.product_id",
                "product_category_name":"s.product_category_name",
                "product_weight_g": "s.product_weight_g",
                "product_length_cm": "s.product_length_cm",
                "product_height_cm": "s.product_height_cm",
                "product_width_cm": "s.product_width_cm",
                "record_hash": "s.record_hash",
                "effective_start_date": "current_timestamp()",
                "effective_end_date": "null",
                "is_current": "Y"
            }
        )
        .execute()
    )

def scd_type2_merge_Seller(spark: SparkSession, source_df:DataFrame, target_path:str):

    delta_table = DeltaTable = DeltaTable.forPath(spark, target_path)
    (
        delta_table.alias("t")
        .merge(
            source_df.alias("s"),
            "t.product_id = s.product_id AND t.is_current = 'Y'"
        )
        .whenMatchedUpdate(
            condition = "t.record_hash != s.record_hash",
            set={
                "effective_end_date": "current_timestamp()",
                "is_current": "N"
            }
        )
        .whenNotMatchedInsert(
            values={
                "product_id": "s.product_id",
                "product_category_name":"s.product_category_name",
                "product_weight_g": "s.product_weight_g",
                "product_length_cm": "s.product_length_cm",
                "product_height_cm": "s.product_height_cm",
                "product_width_cm": "s.product_width_cm",
                "record_hash": "s.record_hash",
                "effective_start_date": "current_timestamp()",
                "effective_end_date": "null",
                "is_current": "Y"
            }
        )
        .execute()
    ) 