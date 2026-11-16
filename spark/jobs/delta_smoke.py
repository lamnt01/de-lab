"""Kiểm tra Delta Lake trên HDFS: ghi bảng, MERGE (upsert), đọc lại phiên bản cũ (time travel).

Chạy: docker exec -it spark-master /opt/spark/bin/spark-submit /opt/spark-jobs/delta_smoke.py
"""
from pyspark.sql import SparkSession

PATH = "hdfs://namenode:8020/lake/_smoke/delta_cities"

spark = SparkSession.builder.appName("delta_smoke").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Version 0: ghi mới
spark.createDataFrame([("hanoi", 24.0), ("hue", 26.0)], "city string, temp double") \
    .write.format("delta").mode("overwrite").save(PATH)

# Version 1: MERGE — hanoi có rồi thì cập nhật, danang chưa có thì thêm
spark.createDataFrame([("hanoi", 25.5), ("danang", 27.0)], "city string, temp double") \
    .createOrReplaceTempView("updates")
spark.sql(f"""
    MERGE INTO delta.`{PATH}` t
    USING updates u ON t.city = u.city
    WHEN MATCHED THEN UPDATE SET t.temp = u.temp
    WHEN NOT MATCHED THEN INSERT *
""")

print("== Hiện tại (version 1) ==")
spark.read.format("delta").load(PATH).orderBy("city").show()

print("== Time travel (version 0) ==")
spark.read.format("delta").option("versionAsOf", 0).load(PATH).orderBy("city").show()

print("== Lịch sử ==")
spark.sql(f"DESCRIBE HISTORY delta.`{PATH}`").select("version", "operation").show()

spark.stop()
