from src.streaming.kafka_reader import read_kafka_stream
from src.streaming.parser import parse_kafka_value
from schemas.all_schema import orders_schema, payment_schema, customer_schema
from src.utils.spark_session import get_spark
from src.sinks.delta_sink import write_to_delta

spark = get_spark("OrdersStreaming")

ollist_orders_df = read_kafka_stream(
    spark,
    topic="olist_orders",
    bootstrap_servers="localhost:9092"
)


olist_customers_df = read_kafka_stream(
    spark,
    topic="olist_customers",
    bootstrap_servers="localhost:9092"
)


olist_payments_df = read_kafka_stream(
    spark,
    topic="olist_payments",
    bootstrap_servers="localhost:9092"
)

orders_stream_df = parse_kafka_value(ollist_orders_df, orders_schema)
payments_stream_df = parse_kafka_value(olist_payments_df, payment_schema)
customers_stream_df = parse_kafka_value(olist_customers_df, customer_schema)


joined_df = payments_stream_df.withWatermark("payment_time", "10 minutes")\
    .join(
        orders_stream_df.withWatermark("order_time", "10 minutes"),
        on="order_id",
        how="inner"
    )\
    .join(
        customers_stream_df,
        on="customer_id",
        how="inner"
    )\
    .select(
        orders_stream_df.order_id,
        orders_stream_df.customer_id,
        customers_stream_df.customer_name,
        customers_stream_df.customer_city,
        orders_stream_df.order_status,
        payments_stream_df.payment_type,
        payments_stream_df.payment_value,
        orders_stream_df.order_time
    )

write_to_delta(joined_df, '/output/olist_orders', '/checkpoints/olist_orders')
#orders_stream.writeStream.format("delta").start().awaitTermination()

