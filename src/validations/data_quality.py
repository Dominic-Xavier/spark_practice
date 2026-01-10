from pyspark.sql.functions import datediff, to_date, col, DataFrame, current_date, row_number
from pyspark.sql.types import StructType
from pyspark.sql.window import Window
from utils.path_utils import resolve_path

def validate_employees(df, logger):
    """
    Employee data quality checks
    """
    # Example:
    # salary > 0
    # email not null and valid format

    valid_records = df.withColumn("Age", datediff(current_date(), to_date(df.dob))) \
        .withColumn("email_valid", col("email").rlike(".+@.+\\..+")) \
        .filter((df.salary > 0) & (df.email.isNotNull()) & (col("email_valid") == True) & (col("Age") >= 18))
    
    bad_record = df.withColumn("Age", datediff(current_date(), to_date(df.dob))) \
        .withColumn("email_valid", col("email").rlike(".+@.+\\..+")) \
        .filter((df.salary <= 0) | (df.email.isNull()) | (col("email_valid") == False) & (col("Age") < 18))
    bad_record.show(5)

    
    if not bad_record.isEmpty():
        logger.warning(f"Found {bad_record.count()} bad employee records is saved in logs/bad_employees.csv")
        bad_record.mode("overwrite").save(resolve_path('\\logs\\bad_employees.csv'))
    else:
        logger.info("No bad employee records found.")

    return valid_records

def validate_schemas(df: DataFrame, expected_schema, logger, strict_types: bool = False) -> DataFrame:
    """
    Generic schema validation:
    - If `expected_schema` is a StructType, ensure every expected field name exists in `df`.
      If `strict_types` is True, also ensure the data types match for the expected fields.
    - If `expected_schema` is a list (or other iterable) of column names, ensure each name exists in `df`.
    - Extra columns present in `df` that are not in `expected_schema` are allowed.
    Raises ValueError on missing expected columns (or type mismatches if strict_types=True).
    Returns the original DataFrame on success.
    """
    actual_fields = {f.name: f.dataType for f in df.schema.fields}

    # Normalize expected schema to dict of name -> dataType (or None if names-only)
    if isinstance(expected_schema, StructType):
        expected_fields = {f.name: f.dataType for f in expected_schema.fields}
    else:
        # assume iterable of names
        expected_fields = {name: None for name in expected_schema}

    # Find missing expected columns
    missing = [name for name in expected_fields.keys() if name not in actual_fields]
    if missing:
        logger.error("Schema validation failed: missing columns.")
        logger.error(f"Missing columns: {missing}")
        raise ValueError(f"Schema validation failed. Missing columns: {missing}")

    # Optionally check types for expected columns (if expected_schema provided as StructType and strict_types=True)
    if strict_types and isinstance(expected_schema, StructType):
        mismatched = []
        for name, expected_type in expected_fields.items():
            actual_type = actual_fields.get(name)
            # direct equality check for simplicity; adjust if you need nullable/metadata-aware comparisons
            if actual_type != expected_type:
                mismatched.append({"column": name, "expected": str(expected_type), "actual": str(actual_type)})
        if mismatched:
            logger.error("Schema validation failed: type mismatches for expected columns.")
            for m in mismatched:
                logger.error(f"{m['column']}: expected={m['expected']}, actual={m['actual']}")
            raise ValueError(f"Schema validation failed. Type mismatches: {mismatched}")

    logger.info("Schema validation passed (no expected columns missing).")
    return df

def deduplicate_bad_records(df: DataFrame, logger) -> DataFrame:
    """
    Deduplicate bad records based on subset of columns
    """
    WindowSpec = Window.partitionBy("department_id", "phone", "email").orderBy(col("department_id"))
    deduplicated_df = df.withColumn("row_num", row_number().over(WindowSpec)) \
        .withColumn("isValid", col("email").rlike(".+@.+\\..+")) \
        .filter((col("row_num") == 1) & (col("isValid") == True)) \
        .drop("row_num")
    
    duplicated_df = df.withColumn("row_num", row_number().over(WindowSpec)) \
        .withColumn("isValid", col("email").rlike(".+@.+\\..+")) \
        .filter((col("row_num") > 1) & (col("isValid") == False)) \
        .drop("row_num")
    
    if not duplicated_df.isEmpty():
        logger.warning(f"Found {duplicated_df.count()} duplicate bad records.")
        duplicated_df.mode("overwrite").save(resolve_path("\\logs\\bad_employees_duplicates.csv"))
        logger.info(
            f"Deduplicated bad records. "
            f"Original count: {df.count()}, "
            f"Deduplicated count: {deduplicated_df.count()}, "
            f"Duplicate records saved to {resolve_path('logs/bad_employees_duplicates.csv')}"
        )
    return deduplicated_df