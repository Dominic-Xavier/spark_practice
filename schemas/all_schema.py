from pyspark.sql.types import *

orders_schema = StructType([
    StructField("order_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("order_status", StringType()),
    StructField("order_time", TimestampType())
])


payment_schema = StructType([
    StructField("order_id", StringType()),
    StructField("payment_type", StringType()),
    StructField("payment_value", DoubleType()),
    StructField("payment_time", TimestampType())
])


customer_schema = StructType([
    StructField("customer_id", StringType()),
    StructField("customer_name", StringType()),
    StructField("customer_city", StringType()),
    StructField("zip_code_prefix", StringType())
])