from utils.spark_session import get_spark
from utils.logger import get_logger
from utils.path_utils import resolve_path

from ingestion import read_records as records
from ingestion import write_records as write

from validations import data_quality as data_check
from transformations import enrich_transactions as tran
from utils.config_loader import load_config
from utils.original_schemas import emp_schema, sales_schema, city_schema

from utils.WaterMarkManager import WaterMarkManager
from pyspark.sql.functions import *
from utils.enum import WriteMode
from utils.runtime_args import get_env_arg


def main():

    
    # ----------------------------
    # Initialize watermark manager
    # ----------------------------
    watermark_manager = WaterMarkManager(resolve_path("\\logs\\watermark\\watermark.json"))

    # ----------------------------
    # Parse arguments & load config
    # ----------------------------
    env = get_env_arg()
    # ----------------------------
    # Initialize Spark & Logger
    #-----------------------------
    spark = get_spark("end_to_end_pipeline")
    logger = get_logger("PIPELINE")

    logger.info(f"Starting pipeline in {env} environment")

    #Load files from yaml file
    config = load_config(env)

    water_mark = WaterMarkManager(resolve_path(config["paths"]["water_mark"]), logger)

    water_mark_value = water_mark.read_watermark("last_processed_date")

    try:
        emp_path = resolve_path(config["paths"]["employees"])
        sales_path = resolve_path(config["paths"]["sales"])

        city_path = resolve_path(config["paths"]["cities"])

    except Exception as e:
        emp_path = config["paths"]["employees"]
        sales_path = config["paths"]["sales"]

        city_path = config["paths"]["cities"]

    
    employees = records.read_records_csv(spark, emp_path)
    transactions = records.read_records_csv(spark, sales_path)
    cities = records.read_records_csv(spark, city_path)

    # Validate schemas
    # allow extra columns, only check presence
    schemas = [emp_schema, sales_schema, city_schema]
    dataframe = [employees, transactions, cities]

    # ----------------------------
    # Schema validation
    # ----------------------------

    logger.info("Validating schemas")
    for expected_schema, actual_df in zip(schemas, dataframe):
        data_check.validate_schemas(actual_df, expected_schema, logger)

    valid_employee_df = (
        employees
        .transform(data_check.validate_employee_email)
        .transform(data_check.deduplicate_employees)
    )

    valid_employee_df.show()

    valid_employee_df = data_check.process_bad_employee_records(
        valid_employee_df,
        logger
    )

    #valid_employee_df = data_check.process_bad_employee_records(deduplicate_employee_df, logger)

    #Working on below method to enrich transactions, inprogress
    employee_dim_df = tran.employee_dim_table(valid_employee_df)
    city_dim_df = tran.city_dim_table(cities)
    sales_fact_df = tran.sales_fact_table(employee_dim_df, transactions)
    # Update watermark after processing
    watermark_manager.update_watermark(last_Processed_date = tran.get_transaction_date(sales_fact_df))
    date_dim_df = tran.date_dim_table(sales_fact_df)
    sales_fact_df = tran.add_year_month_columns(sales_fact_df, "transacted_at")

    # ----------------------------
    # Write data to output paths
    # ----------------------------
    logger.info("Writing data to output paths")

    try:
        write.write_partquest(employee_dim_df,  WriteMode.OVERWRITE, config["output"]["employee_dim"])
        write.write_partquest(city_dim_df, WriteMode.OVERWRITE, config["output"]["city_dim"], "country")
        write.write_partquest(sales_fact_df, WriteMode.OVERWRITE, config["output"]["sales_fact"], "year")
        write.write_partquest(date_dim_df, WriteMode.OVERWRITE, config["output"]["date_dim"])
    except Exception as e:
        write.write_partquest(employee_dim_df,  WriteMode.OVERWRITE, resolve_path(config["output"]["employee_dim"]))
        write.write_partquest(city_dim_df, WriteMode.OVERWRITE, resolve_path(config["output"]["city_dim"]), "country")
        write.write_partquest(sales_fact_df, WriteMode.OVERWRITE, resolve_path(config["output"]["sales_fact"]), "year")
        write.write_partquest(date_dim_df, WriteMode.OVERWRITE, resolve_path(config["output"]["date_dim"]))

    logger.info("Pipeline completed successfully...!")

if __name__ == "__main__":
    main()