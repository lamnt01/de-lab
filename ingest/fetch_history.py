"""Giai đoạn 1: tải dữ liệu thời tiết lịch sử theo giờ từ Open-Meteo về landing zone.

Mỗi lần gọi API = 1 thành phố x 1 năm, lưu nguyên response JSON (không chỉnh sửa) vào:
    data/landing/weather_history/city=<slug>/year=<yyyy>/data.json

Chạy (trong Codespace, thư mục gốc repo):
    python ingest/fetch_history.py --cities hanoi --start-year 2024 --end-year 2024   # thử 1 file
    python ingest/fetch_history.py                                                    # toàn bộ 10 x 21 năm

Sau đó đẩy lên HDFS bronze bằng ingest/load_bronze.sh.
"""
import argparse
import json
import sys
import time
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).parent))
from cities import CITIES  # noqa: E402

ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"

# Giữ <= 10 biến: quá 10 biến thì mỗi request bị tính thêm lượt gọi
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

# Bronze giữ giờ UTC đúng như nguồn; chuyển sang Asia/Ho_Chi_Minh ở bước silver (GĐ2)
TIMEZONE = "GMT"

# Giới hạn gói miễn phí: 600 lượt/phút, 5.000 lượt/giờ, 10.000 lượt/ngày.
# Request dài hơn 2 tuần bị tính nhiều lượt: 1 năm ~ 52 tuần / 2 = ~26 lượt.
# 5.000 / 26 ~ 190 request/giờ  ->  nghỉ ~20 giây giữa 2 request để không vượt giới hạn giờ.
SLEEP_SECONDS = 20

MAX_RETRIES = 3
TIMEOUT_SECONDS = 60
DEFAULT_OUT = Path(__file__).resolve().parent.parent / "data" / "landing" / "weather_history"


def build_params(city: dict, year: int) -> dict:
    """Tham số query cho 1 thành phố trong 1 năm (01/01 -> 31/12)."""
    # TODO 1: trả về dict gồm latitude, longitude, start_date, end_date, hourly, timezone
    #   - start_date/end_date dạng "YYYY-MM-DD"
    #   - hourly: Open-Meteo nhận chuỗi các biến nối bằng dấu phẩy (gợi ý: ",".join(...))
    raise NotImplementedError


def fetch_year(city: dict, year: int) -> dict:
    """Gọi Archive API, trả về response JSON dạng dict. Tự thử lại khi lỗi tạm thời."""
    # TODO 2: gọi requests.get(ARCHIVE_URL, params=build_params(...), timeout=TIMEOUT_SECONDS)
    # TODO 3: thử lại tối đa MAX_RETRIES lần khi:
    #   - lỗi mạng (requests.exceptions.ConnectionError, Timeout)
    #   - status 429 (vượt giới hạn) hoặc 5xx (lỗi phía server)
    #   Chờ tăng dần giữa các lần: time.sleep(2 ** attempt * 5) -> 5s, 10s, 20s
    #   Status 400 (tham số sai) thì KHÔNG thử lại: lỗi do mình, thử lại vẫn sai -> raise luôn
    #   Gợi ý: response.status_code, response.raise_for_status(), response.json()
    # Câu hỏi tự trả lời: vì sao chờ tăng dần thay vì chờ cố định?
    raise NotImplementedError


def output_path(out_dir: Path, city: dict, year: int) -> Path:
    """Đường dẫn file kiểu partition Hive: <out_dir>/city=<slug>/year=<yyyy>/data.json"""
    # TODO 4: dựng Path theo đúng mẫu trên (gợi ý: toán tử / của pathlib)
    # Câu hỏi tự trả lời: vì sao đặt tên thư mục dạng "city=hanoi" thay vì "hanoi"?
    raise NotImplementedError


def save_json(data: dict, path: Path) -> None:
    """Ghi JSON an toàn: không để lại file dở dang nếu chương trình chết giữa chừng."""
    # TODO 5:
    #   - tạo thư mục cha: path.parent.mkdir(parents=True, exist_ok=True)
    #   - ghi ra file tạm cùng thư mục (vd. path.with_suffix(".json.tmp")),
    #     ghi xong mới đổi tên sang path (gợi ý: Path.replace)
    #   - json.dump(..., ensure_ascii=False)
    # Câu hỏi tự trả lời: nếu ghi thẳng vào path mà bị Ctrl+C giữa chừng thì lần chạy sau sẽ sao?
    raise NotImplementedError


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Tải dữ liệu thời tiết lịch sử từ Open-Meteo")
    p.add_argument("--cities", nargs="*", help="slug thành phố, bỏ trống = cả 10")
    p.add_argument("--start-year", type=int, default=2005)
    p.add_argument("--end-year", type=int, default=2025)
    p.add_argument("--out", type=Path, default=DEFAULT_OUT)
    p.add_argument("--force", action="store_true", help="tải lại cả file đã có")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    cities = [c for c in CITIES if not args.cities or c["slug"] in args.cities]
    years = range(args.start_year, args.end_year + 1)
    print(f"{len(cities)} thành phố x {len(years)} năm = {len(cities) * len(years)} request")

    done = skipped = failed = 0
    # TODO 6: lặp qua từng thành phố, từng năm:
    #   - path = output_path(...)
    #   - file đã tồn tại và không có --force -> bỏ qua (skipped += 1), KHÔNG gọi API, KHÔNG sleep
    #   - ngược lại: fetch_year -> save_json -> in ra 1 dòng log (city, year, số dòng = len(data["hourly"]["time"]))
    #   - 1 request lỗi (đã hết lượt thử) thì in lỗi, failed += 1 rồi chạy tiếp, không dừng cả chương trình
    #   - sau mỗi lần GỌI API (kể cả lỗi) thì time.sleep(SLEEP_SECONDS)
    # Câu hỏi tự trả lời: nhờ bước "bỏ qua file đã có", chạy lại script nhiều lần có bị trùng dữ liệu không?

    print(f"Xong: tải {done}, bỏ qua {skipped}, lỗi {failed}")
    if failed:
        sys.exit(1)  # để Airflow (GĐ3) biết task thất bại


if __name__ == "__main__":
    main()
