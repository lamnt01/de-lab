"""10 thành phố trong phạm vi dự án.

slug   : tên không dấu, dùng làm tên thư mục partition (city=<slug>)
region : miền, phục vụ câu hỏi 3 (Đà Lạt, Buôn Ma Thuột thuộc Tây Nguyên, xếp vào miền Trung)
"""

CITIES = [
    {"slug": "hanoi",       "name": "Hà Nội",         "region": "Bắc",   "lat": 21.0285, "lon": 105.8542},
    {"slug": "haiphong",    "name": "Hải Phòng",      "region": "Bắc",   "lat": 20.8449, "lon": 106.6881},
    {"slug": "laocai",      "name": "Lào Cai",        "region": "Bắc",   "lat": 22.4856, "lon": 103.9707},
    {"slug": "hue",         "name": "Huế",            "region": "Trung", "lat": 16.4637, "lon": 107.5909},
    {"slug": "danang",      "name": "Đà Nẵng",        "region": "Trung", "lat": 16.0544, "lon": 108.2022},
    {"slug": "nhatrang",    "name": "Nha Trang",      "region": "Trung", "lat": 12.2388, "lon": 109.1967},
    {"slug": "dalat",       "name": "Đà Lạt",         "region": "Trung", "lat": 11.9404, "lon": 108.4583},
    {"slug": "buonmathuot", "name": "Buôn Ma Thuột",  "region": "Trung", "lat": 12.6667, "lon": 108.0500},
    {"slug": "hcm",         "name": "TP. Hồ Chí Minh", "region": "Nam",  "lat": 10.8231, "lon": 106.6297},
    {"slug": "cantho",      "name": "Cần Thơ",        "region": "Nam",   "lat": 10.0452, "lon": 105.7469},
]
