from utils.spark_session import get_spark
from utils.logger import get_logger
from utils.path_utils import resolve_path

from ingestion import read_records

from validations import data_quality as data_check
from transformations.enrich_transactions import enrich_transactions
from utils.config_loader import load_config
from utils.original_schemas import emp_schema, sales_schema, city_schema
from utils.arg_parser import parse_args
from utils import WaterMarkManager


def main():

    watermark_manager = WaterMarkManager(resolve_path("\\logs\\watermark/watermark.json"))

    args = parse_args()
    spark = get_spark("end_to_end_pipeline")
    logger = get_logger("PIPELINE")

    logger.info("Starting pipeline")

    #Load files from yaml file
    config = load_config(args.env)

    emp_path = resolve_path(config["paths"]["employees"])
    sales_path = resolve_path(config["paths"]["sales"])
    city_path = resolve_path(config["paths"]["cities"])

    employees = read_records.read_employees(spark, emp_path)
    transactions = read_records.read_transactions(spark, sales_path)
    cities = read_records.read_cities(spark, city_path)

    # Validate schemas
    # allow extra columns, only check presence
    schemas = [emp_schema, sales_schema, city_schema]
    dataframe = [employees, transactions, cities]

    for expected_schema, actual_df in zip(schemas, dataframe):
        data_check.validate_schemas(actual_df, expected_schema, logger)

    valid_employees = data_check.validate_employees(employees, logger)
    
    valid_employee_df = data_check.deduplicate_bad_records(valid_employees, logger)
    
    enriched_txn = enrich_transactions(valid_employee_df, cities)

    logger.info("Pipeline completed successfully...!")

if __name__ == "__main__":
    main()