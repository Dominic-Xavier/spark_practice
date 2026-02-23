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

    tar_customer = resolve_path(config['output']['customer_scd_type_2'])
    tar_product = resolve_path(config['output']['product_scd_type_2'])
    tar_seller = resolve_path(config['output']['seller_scd_type_2'])
    
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
    olist_products = scd.product_staging(olist_products_df_dep)
    olist_sellers = scd.seller_staging(olist_sellers_df_dep)

    if DeltaTable.isDeltaTable(spark, tar_customer):
        scd.scd_type2_merge_customer(spark, olist_customer, tar_customer)
    else:
        write.write_parquet_delta(olist_customer, WriteMode.OVERWRITE, tar_customer)
    
    if DeltaTable.isDeltaTable(spark, tar_product):
        scd.scd_type2_merge_Product(spark, olist_products, tar_product)
    else:
        write.write_parquet_delta(olist_products, WriteMode.OVERWRITE, tar_product)
    
    if DeltaTable.isDeltaTable(spark, tar_seller):
        scd.scd_type2_merge_Seller(spark, olist_sellers, tar_seller)
    else:
        write.write_parquet_delta(olist_sellers, WriteMode.OVERWRITE, tar_seller)
    
if __name__ == "__main__":
    main()