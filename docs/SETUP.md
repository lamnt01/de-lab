# Hướng dẫn cài đặt & chạy

Tài liệu chi tiết cho hạ tầng của dự án. Tổng quan dự án xem ở [README](../README.md).

## 1. Cài đặt (làm một lần)

Mở **PowerShell bằng quyền Administrator**:

```powershell
wsl --install            # cài WSL2 + Ubuntu, xong thì RESTART máy
winget install -e --id Docker.DockerDesktop
```

Sau khi restart, mở Docker Desktop và chờ đến khi hiện "Engine running".
Trong Settings → General, nhớ bật "Use the WSL 2 based engine".

Giới hạn RAM cho WSL: tạo file `C:\Users\Ngs-MT681\.wslconfig` với nội dung

```ini
[wsl2]
memory=10GB
processors=6
```

rồi chạy `wsl --shutdown` và mở lại Docker Desktop.

Kiểm tra:

```powershell
docker version
docker compose version
```

### Không có quyền Admin: chạy trên GitHub Codespaces

1. Đẩy thư mục này lên một repo GitHub (để private cũng được).
2. Trên trang repo, bấm **Code → Codespaces → ⋯ → New with options**, chọn máy **4-core**, rồi **Create codespace**.
3. Chờ Codespace dựng xong (lần đầu khoảng 3–5 phút). Mở Terminal ngay trong trình duyệt và chạy các lệnh ở mục 2 bên dưới.
   Thay `docker exec -it ...` bằng đúng lệnh đó, chỉ khác là chạy trong terminal Linux, nên đổi ký tự xuống dòng `` ` `` thành `\`.
4. Mở tab **Ports** để vào các Web UI.
5. Làm xong thì **Stop codespace** (menu Codespaces trên github.com) để không tốn quota.
   Mặc định Codespace tự dừng sau 30 phút không dùng.

Gói miễn phí có 120 core-giờ/tháng. Máy 4-core tương đương khoảng 30 giờ chạy.

## 2. Chạy theo profile

Mọi lệnh dưới đây chạy trong thư mục `C:\Users\Ngs-MT681\de-lab`.

| Profile | Gồm | RAM (ước lượng) |
|---|---|---|
| `hadoop` | HDFS namenode + datanode | ~1 GB |
| `yarn` | ResourceManager + NodeManager | ~1.5 GB |
| `spark` | Spark master + 1 worker (2 core, 2 GB) | ~3 GB |
| `kafka` | Kafka KRaft (1 broker) + Kafka UI | ~1.5 GB |
| `airflow` | Postgres + webserver + scheduler | ~2.5 GB |
| `all` | tất cả trừ yarn | ~8–9 GB |

```powershell
docker compose --profile hadoop --profile spark up -d     # học Spark + HDFS
docker compose --profile kafka up -d                      # học Kafka
docker compose --profile all up -d --build                # lần đầu có airflow nên cần --build
docker compose --profile all ps                           # xem trạng thái
docker compose --profile all down                         # tắt (giữ DB Airflow)
```

Web UI:

| | URL |
|---|---|
| HDFS NameNode | http://localhost:9870 |
| YARN | http://localhost:8088 |
| Spark master | http://localhost:8090 |
| Spark job đang chạy | http://localhost:4040 |
| Kafka UI | http://localhost:8085 |
| Airflow | http://localhost:8080 (airflow / airflow) |

> Dữ liệu HDFS chỉ nằm trong container. `stop`/`start` thì vẫn còn, nhưng `down` thì mất hết.
> Đây là lab nên chấp nhận được; dữ liệu nguồn đã có sẵn trong `./data`.

## 3. Bài thực hành mẫu

### Hadoop: thao tác HDFS
```powershell
docker exec -it namenode hdfs dfs -mkdir -p /lake/raw
docker exec -it namenode hdfs dfs -ls /
docker exec -it namenode hdfs dfsadmin -report
```

### Spark: batch ETL (CSV → Parquet trên HDFS)
```powershell
docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark-jobs/etl_orders.py
docker exec -it namenode hdfs dfs -ls /lake/gold/daily_revenue
```
Muốn gõ lệnh tương tác: `docker exec -it spark-master /opt/spark/bin/pyspark --master spark://spark-master:7077`

### Kafka: tạo topic, gửi và nhận
```powershell
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --topic orders --partitions 3 --replication-factor 1
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic orders --from-beginning
```
Bắn dữ liệu giả, chạy ở một cửa sổ khác:
```powershell
pip install kafka-python-ng
python kafka/producer.py
```

### Spark Structured Streaming đọc Kafka (cần cả profile spark và kafka)
```powershell
docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 `
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 `
  --conf spark.jars.ivy=/tmp/.ivy2 `
  /opt/spark-jobs/stream_orders.py
```

### Airflow: điều phối Spark job
1. Bật profile `airflow` và `spark` (cộng thêm `hadoop` vì job ghi lên HDFS).
2. Vào http://localhost:8080, bật DAG `orders_etl` rồi bấm Trigger.
3. Xem log task `spark_etl`, mở http://localhost:8090 để thấy application đang chạy.

DAG mới chỉ cần đặt file `.py` vào `airflow/dags/`, scheduler sẽ tự nhận.

## 4. Cấu trúc thư mục

```
de-lab/
├── docker-compose.yml
├── hadoop.env            # cấu hình core/hdfs/yarn-site
├── airflow/
│   ├── Dockerfile        # Airflow + Java + Spark provider + pyspark
│   ├── dags/
│   └── logs/
├── spark/jobs/           # mount vào /opt/spark-jobs
├── kafka/producer.py
└── data/orders.csv       # mount vào /opt/data
```

## 5. Lỗi hay gặp

- **Container bị kill / exit 137**: thiếu RAM. Bớt profile hoặc tăng `memory` trong `.wslconfig`.
- **Port đã bị dùng**: sửa vế trái của `ports` trong compose, ví dụ `"18080:8080"`.
- **Spark job từ Airflow treo ở "waiting for resources"**: worker đã hết core vì một job khác (pyspark shell, streaming) đang chiếm. Tắt job đó đi.
- **Python version mismatch**: Airflow image cố ý dùng Python 3.10 để khớp với Spark worker, đừng đổi tag.
