import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import json
import random
import time
from datetime import datetime, timezone

from config.schema import (
    STATUS_VALUES,
    SYNTHETIC_DEVICES,
    UNITS_BY_DEVICE_TYPE,
)


def generate_iot_event(device: dict) -> dict:
    base_values = {
        "temperature": round(random.uniform(18, 30), 2),
        "humidity": round(random.uniform(40, 70), 2),
        "vibration": round(random.uniform(0, 1), 3),
        "co2": random.randint(400, 1200),
        "sound": random.randint(30, 90),
    }

    device_type = device["type"]
    return {
        "device_id": device["device_id"],
        "device_type": device_type,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "value": base_values[device_type],
        "unit": UNITS_BY_DEVICE_TYPE[device_type],
        "status": random.choice(STATUS_VALUES),
    }


if __name__ == "__main__":
    from kafka import KafkaProducer

    from config.logging_setup import setup_logger
    from config.settings import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC

    logger = setup_logger("producer", "producer.log")

    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    logger.info("Starting Kafka producer → topic `%s` @ %s", KAFKA_TOPIC, KAFKA_BOOTSTRAP_SERVERS)

    try:
        while True:
            device = random.choice(SYNTHETIC_DEVICES)
            event = generate_iot_event(device)
            producer.send(KAFKA_TOPIC, value=event)
            logger.info("Sent: %s", event)
            time.sleep(random.uniform(0.5, 2.0))
    except KeyboardInterrupt:
        logger.info("Stopped by user")
    finally:
        producer.close()
