from pyspark.sql import functions as F
from functools import reduce
from pyspark.sql import DataFrame
from datetime import datetime
import boto3, os
from pyspark.sql.types import BooleanType, StructType, StructField, StringType, IntegerType, LongType, DoubleType, TimestampType
from pyspark.sql import SparkSession


def multi_join(base_df, join_configs):
    return reduce(lambda df, cfg: df.join(
        F.broadcast(cfg["df"]) if cfg.get("broadcast", False) else cfg["df"], 
                    cfg["on"], cfg["how"]),
                    join_configs, base_df)

def select_columns(df:DataFrame, *col) -> DataFrame:
    return df.select(*col)

def dateTimeConvert(date_str:str, format_string):
    date_str = date_str.replace("T"," ")
    format_string = '%Y-%m-%d %H:%M:%S'
    date_object = datetime.strptime(date_str, format_string)
    return date_object

def getGlueColumns(database: str, table: str) -> list:

    region = os.environ.get("AWS_REGION", "us-east-2")

    session = boto3.Session(region_name=region)

    boto3_client = session.client('glue')
    response = boto3_client.get_table(
        DatabaseName=database,
        Name=table
    )
    columns = response['Table']['StorageDescriptor']['Columns']
    schema = [
        {"Name": col["Name"], "Type": col["Type"]}
        for col in columns
    ]
    return schema

def glue_to_spark_schema(glue_columns):

    type_map = {
        "string": StringType(),
        "int": IntegerType(),
        "bigint": LongType(),
        "double": DoubleType(),
        "float": DoubleType(),
        "boolean": BooleanType(),
        "timestamp": TimestampType()
    }

    spark_schema = []
    for col in glue_columns:
        spark_schema.append(StructField(col['Name'], type_map.get(col['Type'], StringType()), True))
    return StructType(spark_schema)


def validateSchema(df: DataFrame, expected_schema: dict):

    actual_schema = {field.name: field.dataType.simpleString()
                     for field in df.schema.fields}

    errors = []

    # Check missing columns
    for col, dtype in expected_schema.items():
        if col not in actual_schema:
            errors.append(f"Missing column in Glue table: {col}")
        else:
            actual_dtype = actual_schema[col]

            if actual_dtype != dtype:
                errors.append(
                    f"Datatype mismatch for column {col}. "
                    f"Expected: {dtype}, Found: {actual_dtype}"
                )

    # Check unexpected columns
    for col in actual_schema:
        if col not in expected_schema:
            errors.append(f"Unexpected column in Glue table: {col}")

    if errors:
        raise Exception("Schema validation failed:\n" + "\n".join(errors))


        
def castDataTypeInSchema(df:DataFrame, spark_schema:StructType):
    for field in spark_schema.fields:

        col_name = field.name
        target_type = field.dataType

        if col_name in df.columns:
            df = df.withColumn(col_name, F.col(col_name).cast(target_type))
    return df

def compare_schemas(spark_schema, glue_schema):

    # Convert Spark StructType -> dict
    spark_dict = {}
    for field in spark_schema.fields:
        spark_dict[field.name] = field.dataType.simpleString()

    # Convert Glue schema -> dict
    glue_dict = {}
    for col in glue_schema:
        glue_dict[col["Name"]] = col["Type"]

    errors = []

    # Check for missing columns and datatype mismatch
    for col in spark_dict:

        if col not in glue_dict:
            errors.append(f"{col} missing in Glue Catalog")

        elif spark_dict[col] != glue_dict[col]:
            errors.append(
                f"Datatype mismatch for {col}: Spark={spark_dict[col]} Glue={glue_dict[col]}"
            )

    # Check for extra columns in Glue
    for col in glue_dict:
        if col not in spark_dict:
            errors.append(f"{col} extra column in Glue Catalog")

    if errors:
        print("Schema validation failed:")
        for e in errors:
            print(e)
    else:
        print("Schema validation successful")

    return errors