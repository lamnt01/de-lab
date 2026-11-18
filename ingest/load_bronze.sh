#!/usr/bin/env bash
# Giai đoạn 1: đẩy dữ liệu từ landing zone (máy) lên HDFS bronze.
#   máy:  data/landing/weather_history/city=.../year=.../data.json
#   HDFS: /lake/bronze/weather_history/city=.../year=.../data.json
# Container namenode thấy thư mục ./data của repo tại /opt/data (xem docker-compose.yml).
#
# Chạy: bash ingest/load_bronze.sh      (cần profile hadoop đang chạy)
set -euo pipefail

SRC=/opt/data/landing/weather_history     # đường dẫn TRONG container namenode
DST=/lake/bronze/weather_history          # đường dẫn trên HDFS

# TODO 7: tạo thư mục đích trên HDFS (kể cả thư mục cha)
#   Gợi ý: docker exec namenode hdfs dfs -mkdir -p ...

hdfs() { docker exec namenode hdfs "$@"; }
hdfs dfs -mkdir -p "$DST"

# TODO 8: copy toàn bộ cây thư mục SRC vào DST, ghi đè file trùng tên
#   Gợi ý: hdfs dfs -put có cờ -f (ghi đè). Để ý: put "SRC/*" vào DST khác với put "SRC" vào DST thế nào?

# Dấu * phải được bash TRONG container mở rộng (/opt/data chỉ tồn tại trong namenode)
docker exec namenode bash -c "hdfs dfs -put -f $SRC/* $DST"

# TODO 9: in kết quả để kiểm tra
#   - số thư mục/file/dung lượng: hdfs dfs -count ...
#   - liệt kê vài file:           hdfs dfs -ls -R ... | head

hdfs dfs -count "$DST"                                  # DIR_COUNT FILE_COUNT CONTENT_SIZE PATH
hdfs dfs -ls -R "$DST" | grep data.json | head || true  # || true: head đóng pipe sớm -> SIGPIPE, pipefail sẽ dừng script
hdfs dfs -du -s -h "$DST"
