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
from src.transformations.common_func import multi_join


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
        watermark_manager = WaterMarkManager(config['paths']['water_mark'])
    else:
        watermark_manager = WatermarkReader(config['paths']['water_mark'])

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
    logger.info("Data Quality Checks completed...!")

    # ----------------------------
    # Deduplication
    # ----------------------------

    olist_customers_df_dep = data_check.deduplicate(olist_customers_df, ['customer_id'])
    olist_orders_df_dep = data_check.deduplicate(olist_orders_df, ['order_id'])
    olist_payments_df_dep = data_check.deduplicate(olist_payments_df, ['order_id','payment_sequential'])
    olist_products_df_dep = data_check.deduplicate(olist_products_df, ['product_id'])
    olist_sellers_df_dep = data_check.deduplicate(olist_sellers_df, ['seller_id'])
    logger.info("Deduplication completed...!")


    # ----------------------------
    # Creating Fatct Table
    # ----------------------------

    join_configs = [
        {"df": olist_payments_df_dep, "on": "order_id", "how": "left", "broadcast": True},
        {"df": olist_order_items_df, "on": "order_id", "how": "left", "broadcast": True},
        {"df": olist_products_df_dep, "on": "product_id", "how": "left", "broadcast": True},
        {"df": olist_sellers_df_dep, "on": "seller_id", "how": "left", "broadcast": True},
        {"df": olist_customers_df_dep, "on": "customer_id", "how": "left", "broadcast": True},
    ]

    fact_orders_360_df = multi_join(olist_orders_df_dep,join_configs)
    

    # Write the final DataFrame to the target path
    write.write_parquet_delta(fact_orders_360_df, WriteMode.OVERWRITE, resolve_path(config['output']['fact_orders_360']), "customer_state")
    #fact_orders_360_df.show(truncate=False)
    
    
    # ----------------------------
    # src.transformations
    # ----------------------------

    logger.info("Pipeline completed successfully...!")

if __name__ == "__main__":
    main()