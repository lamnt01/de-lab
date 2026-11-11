"""Batch ETL: đọc CSV đơn hàng, tính doanh thu theo ngày, ghi Parquet lên HDFS."""
import sys

from pyspark.sql import SparkSession, functions as F

src = sys.argv[1] if len(sys.argv) > 1 else "file:///opt/data/orders.csv"
dst = sys.argv[2] if len(sys.argv) > 2 else "hdfs://namenode:8020/lake/gold/daily_revenue"

spark = SparkSession.builder.appName("orders_etl").getOrCreate()

orders = spark.read.option("header", True).option("inferSchema", True).csv(src)

daily = (
    orders.groupBy("order_date")
    .agg(
        F.count("*").alias("orders"),
        F.sum(F.col("quantity") * F.col("unit_price")).alias("revenue"),
    )
    .orderBy("order_date")
)

daily.show()
daily.write.mode("overwrite").parquet(dst)
print(f"Wrote {daily.count()} rows to {dst}")

spark.stop()
