import json
import time
import random
import uuid
from datetime import datetime, timezone
from kafka import KafkaProducer

# Configuration for local Redpanda/Kafka
KAFKA_BROKER = "localhost:19092"
TOPIC_NAME = "gateway_webhooks"


def generate_webhook_event():
    # Random gateway amounts between ₹10 and ₹10,000
    amount_paise = random.randint(1000, 1000000)
    tx_id = f"tx_{uuid.uuid4().hex[:12]}"

    event = {
        "transaction_id": tx_id,
        "amount_paise": amount_paise,  # Keep currency as integers to avoid floating point math errors
        "gateway_status": "SUCCESS",
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "merchant_id": "merch_12345",
    }
    return event


def on_send_success(record_metadata):
    print(
        f"Produced message to {record_metadata.topic} partition [{record_metadata.partition}] @ offset {record_metadata.offset}"
    )


def on_send_error(excp):
    print(f"Error producing message: {excp}")


def main():
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        client_id="python-producer",
        value_serializer=lambda m: json.dumps(m).encode("ascii"),
        key_serializer=lambda k: k.encode("ascii"),
    )

    print(f"Starting webhook stream to {TOPIC_NAME}...")
    try:
        while True:
            event = generate_webhook_event()

            # Produce to Redpanda
            producer.send(
                TOPIC_NAME, key=event["transaction_id"], value=event
            ).add_callback(on_send_success).add_errback(on_send_error)

            # Streaming cadence
            time.sleep(random.uniform(0.1, 1.5))

    except KeyboardInterrupt:
        print("Stopping producer...")
    finally:
        producer.flush()


if __name__ == "__main__":
    main()
