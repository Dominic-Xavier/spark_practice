from pyspark.sql.functions import from_json, col

def parse_kafka_value(df, schema):
    return (
        df.selectExpr("CAST(value AS STRING) as json")
          .select(from_json(col("json"), schema).alias("data"))
          .select("data.*")
    )