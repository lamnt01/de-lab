"""Giai đoạn 2 – Job 1: bronze (JSON gốc) -> silver (Delta, theo giờ, giờ Việt Nam).

Vào : /lake/bronze/weather_history/city=<slug>/year=<yyyy>/data.json
      Mỗi file là 1 object; "hourly" lưu THEO CỘT: {"time": [...8760], "temperature_2m": [...8760], ...}
Ra  : /lake/silver/weather_hourly   (Delta, partition theo year THEO GIỜ VIỆT NAM)
      Mỗi dòng = 1 thành phố x 1 giờ. Khóa: (city, ts_utc)

Chạy: docker exec -it spark-master /opt/spark/bin/spark-submit /opt/spark-jobs/bronze_to_silver.py
"""

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

BRONZE = "hdfs://namenode:8020/lake/bronze/weather_history"
SILVER = "hdfs://namenode:8020/lake/silver/weather_hourly"
LOCAL_TZ = "Asia/Ho_Chi_Minh"
FIRST_DATE, LAST_DATE = "2005-01-01", "2025-12-31"  # phạm vi theo giờ VN

# Các biến theo giờ (khớp HOURLY_VARS trong ingest/fetch_history.py)
HOURLY_VARS = [
    "temperature_2m",
    "relative_humidity_2m",
    "apparent_temperature",
    "precipitation",
    "weather_code",
    "cloud_cover",
    "wind_speed_10m",
    "wind_gusts_10m",
]

# Chép từ ingest/cities.py: container Spark chỉ mount spark/jobs nên không import được file đó
CITY_REGION = {
    "hanoi": "Bắc", "haiphong": "Bắc", "laocai": "Bắc",
    "hue": "Trung", "danang": "Trung", "nhatrang": "Trung", "dalat": "Trung", "buonmathuot": "Trung",
    "hcm": "Nam", "cantho": "Nam",
}  # fmt: skip


def read_bronze(spark: SparkSession) -> DataFrame:
    """Đọc toàn bộ bronze. Kết quả: 210 dòng (1 dòng = 1 file), có thêm cột city, year từ tên thư mục."""
    # TODO 1: đọc JSON với option multiLine=True (mỗi file là 1 object trải nhiều dòng,
    #   không phải JSON Lines). Spark tự sinh cột city, year nhờ partition discovery.
    #   Kiểm tra nhanh: df.count() == 210, df.printSchema() xem hourly là struct<time: array<string>, ...>

    return spark.read.option("multiLine", True).json(BRONZE)


def explode_hourly(df: DataFrame) -> DataFrame:
    """Chuyển 'theo cột' -> 'theo dòng': 210 dòng -> ~1,84 triệu dòng.

    Kết quả mong muốn: city, elevation, time (string), <8 biến trong HOURLY_VARS>
    """
    # TODO 2: ghép các mảng cùng vị trí rồi explode
    #   - F.arrays_zip(F.col("hourly.time"), F.col("hourly.temperature_2m"), ...) tạo
    #     array<struct<time, temperature_2m, ...>>: phần tử thứ i gom giá trị giờ thứ i của mọi biến
    #   - F.explode(...) biến mỗi phần tử mảng thành 1 dòng
    #   - rồi select "h.*" để bung struct ra thành cột
    #   Gợi ý: tạo danh sách cột bằng list comprehension trên ["time"] + HOURLY_VARS
    #   Câu hỏi tự kiểm tra: vì sao KHÔNG explode từng mảng riêng rồi join lại?
    #   Bỏ cột "year" từ tên thư mục: đó là năm GMT, sẽ tính lại năm theo giờ VN ở bước sau.

    # TODO: duyệt ["time"] + HOURLY_VARS
    # dấu * bung list thành từng tham số

    zip_cols = [F.col(f"hourly.{i}") for i in HOURLY_VARS]

    return df.select(
        "city",
        "elevation",
        F.explode(F.arrays_zip(*zip_cols)).alias("h"),
    ).select("city", "elevation", "h.*")


def add_time_columns(df: DataFrame) -> DataFrame:
    """Thêm ts_utc, ts_local, date_local, year, hour và region."""
    # TODO 3: chuỗi time ("2020-01-01T00:00") là giờ GMT
    #   - ts_utc     = F.to_timestamp("time", "yyyy-MM-dd'T'HH:mm")
    #       (main() đã đặt spark.sql.session.timeZone=UTC nên chuỗi được hiểu là giờ UTC.
    #        Nếu không đặt, Spark hiểu theo múi giờ của máy -> lệch giờ âm thầm, rất khó phát hiện)
    #   - ts_local   = F.from_utc_timestamp("ts_utc", LOCAL_TZ)   (giờ đồng hồ tại VN, +7)
    #   - date_local = F.to_date("ts_local"); year = F.year("ts_local"); hour = F.hour("ts_local")
    #   - region: ánh xạ city -> miền từ CITY_REGION
    #       Gợi ý: F.create_map(*[F.lit(x) for kv in CITY_REGION.items() for x in kv])[F.col("city")]
    #       (hoặc tạo DataFrame nhỏ từ dict rồi broadcast join)
    #   - lọc giữ date_local trong [FIRST_DATE, LAST_DATE]: 7 giờ cuối file 2025 thuộc 01/01/2026 giờ VN
    #   - bỏ cột "time" (chuỗi gốc) sau khi đã có ts_utc

    return (
        df.withColumn("ts_utc", F.to_timestamp("time", "yyyy-MM-dd'T'HH:mm"))
        .withColumn("ts_local", F.from_utc_timestamp("ts_utc", LOCAL_TZ))
        .withColumn("date_local", F.to_date("ts_local"))
        .withColumn("year", F.year("ts_local"))
        .withColumn("hour", F.hour("ts_local"))
        .withColumn(
            "region",
            F.create_map(*[F.lit(x) for kv in CITY_REGION.items() for x in kv])[
                F.col("city")
            ],
        )
        .filter(F.col("date_local").between(FIRST_DATE, LAST_DATE))
        .drop("time")
    )


def check_quality(df: DataFrame) -> None:
    """Kiểm tra trước khi ghi; sai thì dừng job (raise) để không ghi dữ liệu hỏng vào silver."""
    # TODO 4: tính và in ra, sai thì raise ValueError
    #   a) trùng khóa: số dòng == số (city, ts_utc) khác nhau
    #   b) null ở ts_utc hoặc temperature_2m: phải = 0
    #   c) số giờ theo (city, year): năm thường 8760, năm nhuận 8784
    #      Ngoại lệ: 2005 thiếu 7 giờ đầu (00:00–06:59 ngày 01/01 giờ VN nằm ở năm 2004 GMT, không tải)
    #      -> in bảng groupBy("city","year").count() rồi lọc các dòng lệch để xem
    #   d) giá trị vô lý: temperature_2m ngoài [-10, 50], precipitation < 0
    #   Gợi ý: gom nhiều phép đếm trong 1 lần agg để Spark chỉ quét dữ liệu 1 lần
    #     df.agg(F.count("*"), F.countDistinct("city", "ts_utc"), F.sum(F.col("x").isNull().cast("int")), ...)

    row_check = df.agg(
        F.count("*").alias("total"),
        F.countDistinct("city", "ts_utc").alias("total_distinct"),
        F.sum(F.col("ts_utc").isNull().cast("int")).alias("num_utc_null"),
        F.sum(F.col("region").isNull().cast("int")).alias("num_region_null"),
        F.sum(F.col("temperature_2m").isNull().cast("int")).alias(
            "num_temperature_2m_null"
        ),
        F.sum(
            ((F.col("temperature_2m") < -10) | (F.col("temperature_2m") > 50)).cast(
                "int"
            )
        ).alias("num_temperature_2m_out_range"),
        F.sum((F.col("precipitation") < 0).cast("int")).alias("num_precipitation_lo_0"),
    ).first()

    counts = (
        df.groupBy("city", "year")
        .count()
        .withColumn(
            "expected",
            F.when(F.col("year") % 4 == 0, 8784)
            .when(F.col("year") == 2005, 8753)
            .otherwise(8760),
        )
    )

    bad = counts.filter(F.col("count") != F.col("expected"))
    print(row_check)

    errors = []
    if row_check["total"] - row_check["total_distinct"] > 0:
        errors.append("Lỗi: Có bản ghi bị trùng")
    elif row_check["num_utc_null"] > 0:
        errors.append("Lỗi: ts_utc có bản ghi null")
    elif row_check["num_temperature_2m_null"] > 0:
        errors.append("Lỗi: temperature_2m có bản ghi null")
    elif row_check["num_temperature_2m_out_range"] > 0:
        errors.append("Lỗi: temperature_2m nằm ngoài khoảng [-10,50]")
    elif row_check["num_precipitation_lo_0"] > 0:
        errors.append("Lỗi: precipitation nhỏ hơn 0")
    elif row_check["num_region_null"] > 0:
        errors.append("Lỗi: region có bản ghi null")
    elif bad.count() > 0:
        bad.show()
        errors.append("Lỗi: số giờ theo thành phố và năm bị thiếu")

    if errors:
        raise ValueError("\n".join(errors))


def write_silver(spark: SparkSession, df: DataFrame) -> None:
    """Ghi Delta, chạy lại nhiều lần không sinh trùng."""
    # TODO 5:
    #   - Lần đầu (bảng chưa có, kiểm tra bằng DeltaTable.isDeltaTable(spark, SILVER)):
    #       df.write.format("delta").partitionBy("year").save(SILVER)
    #   - Các lần sau: MERGE theo khóa (city, ts_utc)
    #       DeltaTable.forPath(spark, SILVER).alias("t")
    #           .merge(df.alias("s"), "<điều kiện khớp khóa>")
    #           .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    #   Câu hỏi tự kiểm tra: MERGE khác mode("overwrite") ở điểm nào khi chỉ nạp lại 1 thành phố?
    if not DeltaTable.isDeltaTable(spark, SILVER):
        df.write.format("delta").partitionBy("year").save(SILVER)
    else:
        DeltaTable.forPath(spark, SILVER).alias("t").merge(
            df.alias("s"), "t.city = s.city AND t.ts_utc = s.ts_utc AND t.year = s.year"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()


def main() -> None:
    spark = (
        SparkSession.builder.appName("bronze_to_silver")
        .config("spark.sql.session.timeZone", "UTC")  # xem TODO 3
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    raw = read_bronze(spark)
    hourly = explode_hourly(raw)
    hourly.printSchema()  # ← thêm tạm; xem tên field có phải time, temperature_2m... không

    silver = add_time_columns(hourly)
    silver.cache()  # check_quality và write_silver cùng dùng -> không tính lại từ JSON 2 lần

    check_quality(silver)
    write_silver(spark, silver)

    # Kiểm tra kết quả: đọc lại bảng, xem vài dòng và lịch sử phiên bản Delta
    out = spark.read.format("delta").load(SILVER)
    print(f"Silver: {out.count():,} dòng")
    out.orderBy("city", "ts_utc").show(5, truncate=False)
    spark.sql(f"DESCRIBE HISTORY delta.`{SILVER}`").select(
        "version", "operation", "operationMetrics.numOutputRows"
    ).show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
