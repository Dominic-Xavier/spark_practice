from utils.spark_session import get_spark
from utils.logger import get_logger
from utils.path_utils import resolve_path

from ingestion import read_records

from validations import data_quality as data_check
from transformations.enrich_transactions import enrich_transactions
from utils.config_loader import load_config
from utils.original_schemas import emp_schema, sales_schema, city_schema
from utils.arg_parser import parse_args
from utils.WaterMarkManager import WaterMarkManager


def main():

    
    # ----------------------------
    # Initialize watermark manager
    # ----------------------------
    #watermark_manager = WaterMarkManager(resolve_path("\\logs\\watermark\\watermark.json"))

    # ----------------------------
    # Parse arguments & load config
    # ----------------------------

    args = parse_args()

    # ----------------------------
    # Initialize Spark & Logger
    #----------------------------
    spark = get_spark("end_to_end_pipeline")
    logger = get_logger("PIPELINE")

    logger.info(f"Starting pipeline in {args.env} environment")

    #Load files from yaml file
    config = load_config(args.env)

    water_mark = WaterMarkManager(resolve_path(config["paths"]["water_mark"]), logger)

    water_mark_value = water_mark.read_watermark("last_processed_date")

    emp_path = resolve_path(config["paths"]["employees"])
    sales_path = resolve_path(config["paths"]["sales"])

    city_path = resolve_path(config["paths"]["cities"])

    employees = read_records.read_records_csv(spark, emp_path)
    transactions = read_records.read_records_csv(spark, sales_path)
    cities = read_records.read_records_csv(spark, city_path)

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
    enrich_transactions(transactions, cities)

    logger.info("Pipeline completed successfully...!")

if __name__ == "__main__":
    main()