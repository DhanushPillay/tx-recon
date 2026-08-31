import json
import time
import random
import uuid
import argparse
from datetime import datetime, timezone
from kafka import KafkaProducer

KAFKA_BROKER = "localhost:19092"
TOPIC_NAME = "gateway_webhooks"


def generate_webhook_event():
    return {
        "transaction_id": f"tx_{uuid.uuid4().hex[:12]}",
        "amount_paise": random.randint(1000, 1000000),
        "gateway_status": "SUCCESS",
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "merchant_id": "merch_12345",
    }


def main():
    parser = argparse.ArgumentParser(description="Kafka Producer Benchmark")
    parser.add_argument(
        "--count", type=int, help="Number of messages to produce", default=100000
    )
    args = parser.parse_args()

    print(f"Initializing KafkaProducer connected to {KAFKA_BROKER}...")
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda m: json.dumps(m).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        linger_ms=5,
        batch_size=65536,
    )

    print(
        f"Starting STRESS TEST mode. Pushing {args.count} messages to {TOPIC_NAME}..."
    )
    start_time = time.time()

    for i in range(1, args.count + 1):
        event = generate_webhook_event()
        producer.send(TOPIC_NAME, key=event["transaction_id"], value=event)

        if i % 10000 == 0:
            print(f"Pushed {i} messages...")

    print("Flushing messages to broker...")
    producer.flush()

    elapsed = time.time() - start_time
    print(
        f"STRESS TEST COMPLETE: {args.count} messages in {elapsed:.2f} seconds ({args.count / elapsed:.2f} msgs/sec)"
    )


if __name__ == "__main__":
    main()
