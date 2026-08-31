import argparse
import random
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer

KAFKA_BROKER = "localhost:19092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"
TOPIC_NAME = "gateway_webhooks"

avro_schema_str = """
{
  "type": "record",
  "name": "WebhookEvent",
  "fields": [
    {"name": "transaction_id", "type": "string"},
    {"name": "amount_paise", "type": "int"},
    {"name": "gateway_status", "type": "string"},
    {"name": "timestamp_utc", "type": "string"},
    {"name": "merchant_id", "type": "string"}
  ]
}
"""


def generate_webhook_event():
    amount_paise = random.randint(1000, 1000000)
    tx_id = f"tx_{uuid.uuid4().hex[:12]}"

    return {
        "transaction_id": tx_id,
        "amount_paise": amount_paise,
        "gateway_status": "SUCCESS",
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "merchant_id": "merch_12345",
    }


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(
            f"Produced record to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}"
        )


def main():
    parser = argparse.ArgumentParser(description="Webhook Producer for Redpanda")
    parser.add_argument(
        "--stress", type=int, help="Run a high-throughput stress test", default=0
    )
    args = parser.parse_args()

    schema_registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})

    avro_serializer = AvroSerializer(
        schema_registry_client, avro_schema_str, lambda event, ctx: event
    )

    producer_conf = {
        "bootstrap.servers": KAFKA_BROKER,
        "key.serializer": StringSerializer("utf_8"),
        "value.serializer": avro_serializer,
        "linger.ms": 50,
        "batch.size": 131072,
        "compression.type": "lz4",
        "acks": "1",
    }

    producer = SerializingProducer(producer_conf)

    if args.stress > 0:
        print(
            f"Starting STRESS TEST mode. Pushing {args.stress} messages to {TOPIC_NAME}..."
        )
        start_time = time.time()
        for i in range(args.stress):
            event = generate_webhook_event()
            producer.produce(topic=TOPIC_NAME, key=event["transaction_id"], value=event)
            producer.poll(0)
            if i > 0 and i % 10000 == 0:
                print(f"Pushed {i} messages...")
        producer.flush()
        elapsed = time.time() - start_time
        print(
            f"STRESS TEST COMPLETE: {args.stress} messages in {elapsed:.2f} seconds ({args.stress / elapsed:.2f} msgs/sec)"
        )
        return

    print(f"Starting webhook stream to {TOPIC_NAME}...")
    try:
        while True:
            event = generate_webhook_event()
            producer.produce(
                topic=TOPIC_NAME,
                key=event["transaction_id"],
                value=event,
                on_delivery=delivery_report,
            )
            producer.poll(0)
            time.sleep(random.uniform(0.1, 1.5))
    except KeyboardInterrupt:
        print("Stopping producer...")
    finally:
        producer.flush()


if __name__ == "__main__":
    main()
