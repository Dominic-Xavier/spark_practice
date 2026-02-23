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

from src.transformations.enrichment import scdLogic as scd

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
    
    watermark_manager.read_watermark("customer_count")

    olist_customers_df = records.read_records_csv(spark, resolve_path(config['paths']['olist_customers']), oil_Customers_schema)
    olist_products_df = records.read_records_csv(spark, resolve_path(config['paths']['olist_products']), oil_Products_schema)
    olist_sellers_df = records.read_records_csv(spark, resolve_path(config['paths']['olist_sellers']), oil_Sellers_schema)

    # ----------------------------
    # Data Quality Checks
    # ----------------------------

    olist_customers_df = data_check.check_nulls(olist_customers_df, ['customer_id'], 'olist_customers')
    olist_products_df = data_check.check_nulls(olist_products_df, ['product_id'], 'olist_products')
    olist_sellers_df = data_check.check_nulls(olist_sellers_df, ['seller_id'], 'olist_sellers')

    # ----------------------------
    # Deduplication
    # ----------------------------

    olist_customers_df_dep = data_check.deduplicate(olist_customers_df, ['customer_id'])
    olist_products_df_dep = data_check.deduplicate(olist_products_df, ['product_id'])
    olist_sellers_df_dep = data_check.deduplicate(olist_sellers_df, ['seller_id'])

    olist_customer = scd.customer_staging(olist_customers_df_dep)
    olist_products = scd.customer_staging(olist_products_df_dep)
    olist_sellers = scd.customer_staging(olist_sellers_df_dep)

    scd.scd_type2_merge_customer(spark, olist_customer, config['output']['customer_scd_type_2'])
    scd.scd_type2_merge_Product(spark, olist_products, config['output']['product_scd_type_2'])
    scd.scd_type2_merge_Seller(spark, olist_sellers, config['output']['seller_scd_type_2'])
    
if __name__ == "__main__":
    main()