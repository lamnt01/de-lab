"""Structured Streaming: đọc topic Kafka 'orders', tính doanh thu theo cửa sổ 1 phút, in ra console."""
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

schema = StructType([
    StructField("order_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("product", StringType()),
    StructField("quantity", IntegerType()),
    StructField("unit_price", DoubleType()),
    StructField("event_time", StringType()),
])

spark = SparkSession.builder.appName("stream_orders").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "orders")
    .option("startingOffsets", "earliest")
    .load()
)

orders = (
    raw.select(F.from_json(F.col("value").cast("string"), schema).alias("o"))
    .select("o.*")
    .withColumn("event_time", F.to_timestamp("event_time"))
)

revenue = (
    orders.withWatermark("event_time", "2 minutes")
    .groupBy(F.window("event_time", "1 minute"), "product")
    .agg(F.sum(F.col("quantity") * F.col("unit_price")).alias("revenue"))
)

query = (
    revenue.writeStream.outputMode("update")
    .format("console")
    .option("truncate", False)
    .option("checkpointLocation", "/tmp/checkpoints/stream_orders")
    .start()
)
query.awaitTermination()
