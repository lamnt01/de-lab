# 🌦️ Vietnam Weather Data Pipeline

Pipeline dữ liệu end-to-end phân tích **khí hậu 20 năm (2005–2025)** và **cảnh báo thời tiết real-time** cho 10 thành phố Việt Nam, xây dựng theo kiến trúc **lakehouse** (HDFS + Delta Lake) với Spark, Kafka và Airflow.

![Hadoop](https://img.shields.io/badge/Hadoop-3.3.6-66CCFF?logo=apachehadoop&logoColor=black)
![Spark](https://img.shields.io/badge/Spark-3.5.3-E25A1C?logo=apachespark&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta_Lake-3.3.3-00ADD4?logo=delta&logoColor=white)
![Kafka](https://img.shields.io/badge/Kafka-3.9.0-231F20?logo=apachekafka&logoColor=white)
![Airflow](https://img.shields.io/badge/Airflow-2.10.5-017CEE?logo=apacheairflow&logoColor=white)
![Docker](https://img.shields.io/badge/Docker_Compose-2496ED?logo=docker&logoColor=white)

> 🚧 **Đang phát triển.** Đã xong hạ tầng và ingest dữ liệu lịch sử vào HDFS bronze; đang làm Giai đoạn 2 (transform). Tiến độ chi tiết ở mục [Lộ trình](#️-lộ-trình).

## ✨ Điểm nổi bật

- **Batch:** xử lý ~1,84 triệu bản ghi thời tiết theo giờ bằng Spark, lưu trên lakehouse (HDFS + Delta Lake) theo kiến trúc medallion: có ACID, MERGE và time travel ngay trên data lake.
- **Streaming:** thu thập thời tiết hiện tại mỗi 15 phút qua Kafka, Spark Structured Streaming phát cảnh báo nắng nóng, mưa lớn, gió mạnh.
- **Orchestration:** Airflow điều phối backfill 20 năm và pipeline hằng ngày (ingest → transform → kiểm tra chất lượng → báo cáo).
- **Hạ tầng dạng code:** toàn bộ cụm chạy bằng Docker Compose, mở được trên GitHub Codespaces mà không cần cài đặt.

## 🏗️ Kiến trúc

```mermaid
flowchart LR
    subgraph SRC[Nguồn: Open-Meteo API]
        A1[Archive API<br/>lịch sử theo giờ]
        A2[Forecast API<br/>hiện tại, 15 phút/lần]
    end

    subgraph BATCH[Batch]
        I[Ingest job] --> L[landing<br/>file tạm]
        L --> B[(bronze<br/>JSON gốc trên HDFS)]
        B --> S1[Spark] --> SV[(silver<br/>Delta table)]
        SV --> S2[Spark] --> G[(gold<br/>Delta: fact / dim)]
        G --> R[📊 Báo cáo khí hậu<br/>HTML]
    end

    subgraph STREAM[Streaming]
        P[Producer] --> K{{Kafka<br/>weather.current}}
        K --> SS[Spark Structured<br/>Streaming]
        SS --> KA{{Kafka<br/>weather.alerts}}
        SS --> B
    end

    A1 --> I
    A2 --> P
    AF[Airflow] -. điều phối .-> I & S1 & S2 & R
```

| Công cụ | Vai trò trong dự án |
|---|---|
| **HDFS** | Lưu trữ của lakehouse, 3 tầng: `bronze` (dữ liệu gốc) → `silver` (đã làm sạch, partition theo năm) → `gold` (bảng phục vụ báo cáo) |
| **Delta Lake** | Table format cho silver/gold: ghi ACID, `MERGE` để nạp lại dữ liệu không trùng, schema enforcement, time travel |
| **Spark** | Làm sạch, chuẩn hóa múi giờ, tổng hợp ngày/tháng/năm, tính xu hướng và bất thường |
| **Kafka** | Nhận dữ liệu thời tiết hiện tại; key theo thành phố để giữ thứ tự trong từng partition |
| **Spark Structured Streaming** | Cửa sổ thời gian theo event time + watermark, phát cảnh báo khi vượt ngưỡng |
| **Airflow** | Lập lịch, phụ thuộc giữa các task, backfill theo năm, retry |
| **Docker Compose** | Dựng cụm theo profile (`hadoop`, `spark`, `kafka`, `airflow`) để tiết kiệm tài nguyên |

## ❓ Câu hỏi phân tích

| # | Câu hỏi | Kết quả |
|---|---|---|
| 1 | Mỗi thành phố đã nóng lên bao nhiêu °C trong 20 năm? | _đang thực hiện_ |
| 2 | Số ngày nắng nóng ≥ 35°C mỗi năm có xu hướng tăng không? | _đang thực hiện_ |
| 3 | Mùa mưa ba miền Bắc – Trung – Nam lệch nhau thế nào? | _đang thực hiện_ |
| 4 | Các đợt mưa cực đoan (> 100 mm/ngày) xảy ra khi nào? | _đang thực hiện_ |
| 5 | Tháng nào bất thường nhất so với trung bình nhiều năm? | _đang thực hiện_ |
| 6 | Thành phố nào có thời tiết dễ chịu nhất quanh năm? | _đang thực hiện_ |

**Phạm vi:** Hà Nội, Hải Phòng, Lào Cai, Huế, Đà Nẵng, Nha Trang, Đà Lạt, Buôn Ma Thuột, TP.HCM, Cần Thơ.

## 🗂️ Mô hình dữ liệu

**Medallion trên lakehouse (HDFS + Delta Lake)**

| Tầng | Định dạng | Nội dung |
|---|---|---|
| `landing` | JSON (đĩa máy chạy ingest) | Chỗ dừng tạm trước khi đẩy lên HDFS |
| `bronze/` | JSON | Response gốc từ API, không chỉnh sửa, phục vụ chạy lại |
| `silver/` | Delta, partition theo `year` | Dữ liệu theo giờ đã làm sạch, ép kiểu, chuẩn múi giờ `Asia/Ho_Chi_Minh`; nạp bằng `MERGE` nên chạy lại không trùng |
| `gold/` | Delta | Star schema phục vụ báo cáo |

Bronze giữ nguyên file JSON (bất biến, chỉ thêm mới) nên chưa cần table format; Delta bắt đầu từ silver, nơi dữ liệu được cập nhật và đọc nhiều.
Bảng Delta được truy cập theo đường dẫn (``delta.`/lake/silver/...` ``), chưa dùng Hive Metastore.

**Star schema (gold)**

```mermaid
erDiagram
    dim_location ||--o{ fact_weather_daily : ""
    dim_date ||--o{ fact_weather_daily : ""
    dim_location {
        int location_key
        string city
        string region
        double latitude
        double longitude
        int elevation_m
    }
    dim_date {
        int date_key
        date full_date
        int year
        int month
        string season
    }
    fact_weather_daily {
        int date_key
        int location_key
        double temp_min
        double temp_max
        double temp_mean
        double precipitation_sum
        double wind_max
        boolean is_hot_day
    }
```

## 🚀 Chạy nhanh

Cách nhanh nhất là **GitHub Codespaces** (không cần cài gì): bấm **Code → Codespaces → New with options → 2-core**, sau đó:

```bash
docker compose --profile hadoop --profile spark up -d
docker compose ps
```

Chạy trên máy cá nhân với Docker Desktop, danh sách profile, Web UI và xử lý lỗi: xem [docs/SETUP.md](docs/SETUP.md).

## 📁 Cấu trúc thư mục

```text
de-lab/
├── docker-compose.yml      # Cụm Hadoop, Spark, Kafka, Airflow (chia theo profile)
├── hadoop.env              # Cấu hình core-site / hdfs-site / yarn-site
├── .devcontainer/          # Cấu hình GitHub Codespaces
├── airflow/
│   ├── Dockerfile          # Airflow + Java + Spark provider
│   └── dags/               # DAG điều phối pipeline
├── ingest/                 # Tải dữ liệu Open-Meteo → landing → HDFS bronze
├── spark/conf/             # spark-defaults.conf: HDFS, Delta Lake, Kafka connector
├── spark/jobs/             # Spark batch & streaming jobs
├── kafka/                  # Producer
├── data/                   # Dữ liệu mẫu
└── docs/                   # Tài liệu cài đặt, hình ảnh
```

## 🗺️ Lộ trình

- [x] **Giai đoạn 0 – Hạ tầng:** Docker Compose cho HDFS, Spark, Kafka, Airflow; chạy trên Codespaces
- [x] **Giai đoạn 1 – Ingest:** nạp dữ liệu lịch sử từ Open-Meteo vào HDFS bronze
  - 210 file JSON (10 thành phố × 21 năm, 2005–2025), ~1,84 triệu bản ghi theo giờ, ~105 MB, partition kiểu Hive `city=<slug>/year=<yyyy>`
  - Ingest có retry khi timeout, ghi file atomic (file tạm rồi đổi tên), bỏ qua file đã tải nên chạy lại được an toàn
- [ ] **Giai đoạn 2 – Transform:** Spark bronze → silver → gold (Delta Lake), trả lời câu hỏi 1–3
- [ ] **Giai đoạn 3 – Orchestration:** DAG backfill + DAG hằng ngày, sinh báo cáo HTML
- [ ] **Giai đoạn 4 – Streaming:** Kafka producer + Spark Streaming phát cảnh báo
- [ ] **Giai đoạn 5 – Hoàn thiện:** câu hỏi 4–6, star schema, kiểm tra chất lượng dữ liệu

## 📚 Nguồn dữ liệu

Dữ liệu thời tiết từ [Open-Meteo](https://open-meteo.com/) (Historical Weather API dựa trên ERA5 reanalysis và Forecast API), giấy phép [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/).
Số liệu là dữ liệu mô hình theo ô lưới nên có thể lệch nhẹ so với trạm đo thực tế.

## 👤 Tác giả

[lamntg3625@gmail.com](mailto:lamntg3625@gmail.com) · [GitHub @lamnt01](https://github.com/lamnt01)
