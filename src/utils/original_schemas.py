from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType, DateType

emp_schema = StructType([
    StructField("first_name", StringType(), False),
    StructField("last_name", StringType(), False),
    StructField("job_title", StringType(), True),
    StructField("dob", DateType(), False),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("salary", IntegerType(), True),
    StructField("department_id", IntegerType(), False)
])

sales_schema = StructType([
    StructField("transacted_at", TimestampType(), False),
    StructField("trx_id", IntegerType(), True),
    StructField("retailer_id", IntegerType(), True),
    StructField("description", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("city_id", IntegerType(), False)
])

city_schema = StructType([
    StructField("city_id", IntegerType(), False),
    StructField("city", StringType(), False),
    StructField("state", StringType(), False),
    StructField("state_abv", StringType(), True),
    StructField("country", StringType(), False)
])