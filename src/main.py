from datetime import datetime
from utils.spark_session import get_spark
from utils.logger import get_logger
from utils.path_utils import resolve_path

from ingestion import read_records as records
from ingestion import write_records as write

from validations import data_quality as data_check
from transformations import enrich_transactions as tran
from transformations import scd_employee as scd_emp
from utils.config_loader import load_config
from utils.original_schemas import emp_schema, sales_schema, city_schema

from utils.WaterMarkManager import WaterMarkManager
from utils.WatermarkReader import WatermarkReader
from pyspark.sql.functions import *
from utils.enum import WriteMode
from utils.runtime_args import get_env_arg


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

    logger.info(f"Starting pipeline in {env} environment")

    #Load files from yaml file
    config = load_config(env)

    if env == "prod":
        employees = records.read_records_csv(spark, config["paths"]["employees"])
        transactions = records.read_records_csv(spark, config["paths"]["sales"])
        cities = records.read_records_csv(spark, config["paths"]["cities"])
        water_mark = WatermarkReader(config["paths"]["water_mark"])

    else:
        employees = records.read_records_csv(spark, resolve_path(config["paths"]["employees"]))
        transactions = records.read_records_csv(spark, resolve_path(config["paths"]["sales"]))
        cities = records.read_records_csv(spark, resolve_path(config["paths"]["cities"]))
        water_mark = WaterMarkManager(resolve_path(config["paths"]["water_mark"]), logger)
    # ----------------------------
    # Read input data
    # ----------------------------
    logger.info("Reading input data")

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

    valid_employee_df = data_check.process_bad_employee_records(
        valid_employee_df,
        logger
    )

    # valid_employee_df = data_check.process_bad_employee_records(deduplicate_employee_df, logger)
    logger.info("Transforming data")

    employee_dim_df = tran.employee_dim_table(valid_employee_df)
    city_dim_df = tran.city_dim_table(cities)
    
    sales_fact_df = tran.sales_fact_table(employee_dim_df, transactions)

    date_dim_df = tran.date_dim_table(sales_fact_df)
    sales_fact_df = tran.add_year_month_columns(sales_fact_df, "transacted_at")

    if env == "prod":
        emp_din = config["output"]["employee_dim"]
        city_dim = config["output"]["city_dim"]
        sales_fact = config["output"]["sales_fact"]
        date_dim = config["output"]["date_dim"]
    else:
        emp_din = resolve_path(config["output"]["employee_dim"])
        city_dim = resolve_path(config["output"]["city_dim"])
        sales_fact = resolve_path(config["output"]["sales_fact"])
        date_dim = resolve_path(config["output"]["date_dim"])

    # Working on below method to enrich transactions, inprogress
    sales_last_processed = datetime.combine(tran.get_transaction_date(sales_fact_df), datetime.min.time())
    print(sales_last_processed)
    waterMark_data = water_mark.read_watermark("last_Processed_date")
    if waterMark_data is not None:
        watermark_data = datetime.strptime(waterMark_data, "%Y-%m-%d")
    else:
        watermark_data = None
    print(waterMark_data)
    if (watermark_data is not None and
        watermark_data < sales_last_processed):

        # ----------------------------
        # Write data to output paths
        # ----------------------------
        logger.info("Writing data to output paths")
        
        target_sales_df = records.read_records_parquet(spark, sales_fact)
        target_employee_df = records.read_records_parquet(spark, emp_din)

        staging_sales_df = tran.prepare_sales_staging(sales_fact_df)
        sales_Fact_Updated_Record = tran.scd1_sales_merge(staging_sales_df, target_sales_df)

        scd_emp.prepare_employee_staging(valid_employee_df)
        employee_scd2_dim_df = scd_emp.scd2_employee_merge(staging_sales_df, target_employee_df)
        
        write.write_partquest(employee_scd2_dim_df,  WriteMode.OVERWRITE, emp_din, "department_id")
        write.write_partquest(city_dim_df, WriteMode.OVERWRITE, city_dim, "country")
        write.write_partquest(sales_Fact_Updated_Record, WriteMode.OVERWRITE, sales_fact, "year", "month")
        write.write_partquest(date_dim_df, WriteMode.OVERWRITE, date_dim)

        # Update watermark after processing
        water_mark.update_watermark(last_Processed_date = sales_last_processed)
    
    elif watermark_data is None:

        # ----------------------------
        # Write data to output paths
        # ----------------------------

        logger.info("Writing data to output paths")

        prepare_sales_staging_df = tran.prepare_sales_staging(sales_fact_df)
        prepare_employee_staging_df = scd_emp.prepare_employee_staging(valid_employee_df)
        
        write.write_partquest(prepare_employee_staging_df,  WriteMode.OVERWRITE, emp_din, "department_id")
        write.write_partquest(city_dim_df, WriteMode.OVERWRITE, city_dim, "country")
        write.write_partquest(prepare_sales_staging_df, WriteMode.OVERWRITE, sales_fact, "year", "month")
        write.write_partquest(date_dim_df, WriteMode.OVERWRITE, date_dim)

        # Update watermark after processing
        water_mark.update_watermark(last_Processed_date = sales_last_processed)

    else:
        logger.info("No new transactions to process based on watermark.")

    logger.info("Pipeline completed successfully...!")

if __name__ == "__main__":
    main()