from src.utils.spark_session import get_spark
from src.utils.logger import get_logger
from src.utils.path_utils import resolve_path

from src.ingestion import read_records as records
from src.ingestion import write_records as write

from src.validations import data_quality as data_check
from src.utils.config_loader import load_config
from src.utils.original_schemas import *

from src.utils.WaterMarkManager import WaterMarkManager
from src.utils.WatermarkReader import WatermarkReader
from pyspark.sql.functions import *
from src.utils.enum import WriteMode
from src.utils.runtime_args import get_env_arg
from src.transformations import common_func as com_fun
from src.transformations.enrichment import enrich_order as en_order
from delta.tables import DeltaTable


def main():
    # ------------------------------
    # Parse arguments & load config
    # ------------------------------

    env = get_env_arg()

    # ----------------------------
    # Initialize Spark & Logger
    #-----------------------------

    spark = get_spark("end_to_end_pipeline")
    logger = get_logger("PIPELINE")

    config = load_config(env)

    logger.info("Pipeline started...!")

    # ----------------------------
    # Watermark Management
    # ----------------------------
    if env == 'dev':
        watermark_manager = WaterMarkManager(resolve_path(config['paths']['water_mark']))
    else:
        watermark_manager = WatermarkReader(resolve_path(config['paths']['water_mark']))
    max_ts = None
    max_time = watermark_manager.read_watermark("order_purchase_timestamp")
    if max_time:
        max_ts = com_fun.dateTimeConvert(max_time, '%Y-%m-%d %H:%M:%S')

    # ----------------------------
    # Ingestion
    # ----------------------------

    olist_customers_df = records.read_records_csv(spark, resolve_path(config['paths']['olist_customers']), oil_Customers_schema)
    olist_orders_df = records.read_records_csv(spark, resolve_path(config['paths']['olist_orders']), oil_Orders_schema)
    olist_payments_df = records.read_records_csv(spark, resolve_path(config['paths']['olist_payments']), oil_OrderPayments_schema)
    olist_geolocation_df = records.read_records_csv(spark, resolve_path(config['paths']['olist_geolocation']), oil_GeoLocation_schema)
    olist_order_items_df = records.read_records_csv(spark, resolve_path(config['paths']['olist_order_items']), oil_OrderItems_schema)
    olist_products_df = records.read_records_csv(spark, resolve_path(config['paths']['olist_products']), oil_Products_schema)
    olist_sellers_df = records.read_records_csv(spark, resolve_path(config['paths']['olist_sellers']), oil_Sellers_schema)
    olist_order_reviews_df = records.read_records_csv(spark, resolve_path(config['paths']['olist_order_reviews']), oil_OrderReviews_schema)
    logger.info("Ingestion completed...!")

    # ----------------------------
    # Data Quality Checks
    # ----------------------------

    olist_customers_df = data_check.check_nulls(olist_customers_df, ['customer_id'], 'olist_customers')
    olist_orders_df = data_check.check_nulls(olist_orders_df, ['order_id'], 'olist_orders')
    olist_payments_df = data_check.check_nulls(olist_payments_df, ['order_id'], 'olist_payments')
    olist_products_df = data_check.check_nulls(olist_products_df, ['product_id'], 'olist_products')
    olist_sellers_df = data_check.check_nulls(olist_sellers_df, ['seller_id'], 'olist_sellers')
    olist_order_reviews = data_check.check_nulls(olist_order_reviews_df, ['order_id'], 'olist_order_reviews')
    logger.info("Data Quality Checks completed...!")

    # ----------------------------
    # Deduplication
    # ----------------------------

    olist_customers_df_dep = data_check.deduplicate(olist_customers_df, ['customer_id'])
    olist_orders_df_dep = data_check.deduplicate(olist_orders_df, ['order_id'])
    olist_payments_df_dep = data_check.deduplicate(olist_payments_df, ['order_id','payment_sequential'])
    olist_products_df_dep = data_check.deduplicate(olist_products_df, ['product_id'])
    olist_sellers_df_dep = data_check.deduplicate(olist_sellers_df, ['seller_id'])
    olist_order_reviews_df_dep = data_check.deduplicate(olist_order_reviews, ['order_id'])
    logger.info("Deduplication completed...!")

    target_path = resolve_path(config['output']['fact_orders_360'])

    olist_order_items = en_order.total_items(olist_order_items_df)
    olist_orders = en_order.delivery_days(olist_orders_df_dep)

    olist_order_items_df_final = olist_orders.join(broadcast(olist_order_items), on="order_id", how="left")

    # ----------------------------
    # Creating Fatct Table
    # ----------------------------

    '''join_configs = [
        {"df": olist_payments_df_dep, "on": "order_id", "how": "left", "broadcast": True},
        {"df": olist_order_items, "on": "order_id", "how": "left", "broadcast": True},
        {"df": olist_products_df_dep, "on": "product_id", "how": "left", "broadcast": True},
        {"df": olist_sellers_df_dep, "on": "seller_id", "how": "left", "broadcast": True},
        {"df": olist_customers_df_dep, "on": "customer_id", "how": "left", "broadcast": True},
    ]'''

    join_configs = [
        {"df": olist_order_items_df_final, "on": "order_id", "how": "left", "broadcast": True},
        {"df": olist_products_df_dep, "on": "product_id", "how": "left", "broadcast": True},
        {"df": olist_sellers_df_dep, "on": "seller_id", "how": "left", "broadcast": True},
        {"df": olist_payments_df_dep, "on": "order_id", "how": "left", "broadcast": True},
        {"df": olist_customers_df_dep, "on": "customer_id", "how": "left", "broadcast": True},
        {"df": olist_order_reviews_df_dep, "on": "order_id", "how": "left", "broadcast": True}
    ]

    fact_orders_360 = com_fun.multi_join(olist_order_items_df,join_configs)
    fact_orders_360_df = com_fun.select_columns(fact_orders_360, "customer_id", "order_id", "customer_city", "customer_state", "seller_id", "product_id",
        "total_items", "total_order_value", "payment_type", "review_score", "delivery_days", "order_purchase_timestamp")
    
    staging_df = en_order.prepare_Fact_360_staging(fact_orders_360_df)

    if max_ts is not None:
        incremental_df = staging_df.filter(col("order_purchase_timestamp") > max_ts)
    else:
        incremental_df = staging_df
    
    incremental_df.show()

    if incremental_df.rdd.isEmpty():
        logger.info("No new data")
        logger.info("Pipeline completed successfully...!")
        return
    

    # 2️⃣ First run vs subsequent run
    if not DeltaTable.isDeltaTable(spark, target_path):
        write.write_parquet_delta(incremental_df, WriteMode.OVERWRITE, target_path, "customer_state")
    else:
        en_order.upsert(spark, incremental_df, target_path)

    # 3️⃣ Update watermark
    new_max_ts = incremental_df.agg(
        max("order_purchase_timestamp")
    ).first()[0]

    watermark_manager.update_watermark(order_purchase_timestamp=new_max_ts)

    logger.info("Pipeline completed successfully...!")
    
    '''
    en_order.upsert(spark, incremental_df, target_df)
    
    max_time_ts = incremental_df.agg(max(col("order_purchase_timestamp")).alias("max_time_ts"))
    watermark_manager.update_watermark(order_purchase_timestamp = max_time_ts)
    '''

if __name__ == "__main__":
    main()