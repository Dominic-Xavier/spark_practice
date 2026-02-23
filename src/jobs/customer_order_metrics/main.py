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
from src.transformations.enrichment import enrich_customer as enrich_cus

def main():
    # ------------------------------
    # Parse arguments & load config
    # ------------------------------

    env = get_env_arg()

    # ----------------------------
    # Initialize Spark & Logger
    #-----------------------------

    spark = get_spark("Customer Order Metrices")
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

    # ----------------------------
    # Ingestion
    # ----------------------------

    olist_customer_data = records.read_records_parquet(spark, resolve_path(config['output']['fact_orders_360']))
    olist_customer_data.printSchema()
    #print(olist_customer_data.printSchema())

    # ----------------------------
    # Enrich Transactions
    # ----------------------------

    dim_customer_order_metrics = enrich_cus.dim_customer_order_metrics_sql(spark, olist_customer_data)
    write.write_parquet_delta(dim_customer_order_metrics, WriteMode.OVERWRITE, resolve_path(config['output']['dim_customer_order_metrics']))

if __name__ == "__main__":
    main()