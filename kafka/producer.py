"""Bắn đơn hàng giả vào topic 'orders'.

Chạy trên Windows:   pip install kafka-python-ng  rồi  python kafka/producer.py
Chạy trong Docker:   docker exec -it -e KAFKA_BOOTSTRAP=kafka:9092 airflow-scheduler python /opt/kafka-scripts/producer.py
"""
import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9094")
PRODUCTS = {"Laptop": 1500.0, "Mouse": 25.0, "Keyboard": 70.0, "Monitor": 300.0, "Headset": 90.0}

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    key_serializer=lambda k: k.encode(),
    value_serializer=lambda v: json.dumps(v).encode(),
)

print(f"Sending to {BOOTSTRAP}, Ctrl+C to stop")
try:
    while True:
        product = random.choice(list(PRODUCTS))
        order = {
            "order_id": str(uuid.uuid4()),
            "customer_id": f"C{random.randint(1, 50):03d}",
            "product": product,
            "quantity": random.randint(1, 5),
            "unit_price": PRODUCTS[product],
            "event_time": datetime.now(timezone.utc).isoformat(),
        }
        # key = customer_id: đơn của cùng một khách luôn vào cùng partition
        producer.send("orders", key=order["customer_id"], value=order)
        print(order)
        time.sleep(1)
except KeyboardInterrupt:
    pass
finally:
    producer.flush()
    producer.close()
