import argparse
import logging
import random
import time
import uuid
from datetime import UTC, datetime

from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer

from src.common.schemas import WEBHOOK_AVRO_SCHEMA
from src.common.settings import get_settings

logger = logging.getLogger(__name__)


def generate_webhook_event():
    amount_paise = random.randint(1000, 1000000)
    tx_id = f"tx_{uuid.uuid4().hex[:12]}"

    return {
        "transaction_id": tx_id,
        "amount_paise": amount_paise,
        "gateway_status": "SUCCESS",
        "timestamp_utc": datetime.now(UTC).isoformat(),
        "merchant_id": "merch_12345",
        "processing_run_id": None,
    }


def delivery_report(err, msg):
    if err is not None:
        logger.error(f"Delivery failed: {err}")
    else:
        logger.info(f"Produced record to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")


def main():
    settings = get_settings()
    parser = argparse.ArgumentParser(description="Webhook Producer for Redpanda")
    parser.add_argument("--stress", type=int, help="Run a high-throughput stress test", default=0)
    args = parser.parse_args()

    schema_registry_client = SchemaRegistryClient({"url": settings.schema_registry_url})

    avro_serializer = AvroSerializer(
        schema_registry_client, WEBHOOK_AVRO_SCHEMA, lambda event, ctx: event
    )

    producer_conf = {
        "bootstrap.servers": settings.kafka_broker,
        "key.serializer": StringSerializer("utf_8"),
        "value.serializer": avro_serializer,
        "linger.ms": 50,
        "batch.size": 131072,
        "compression.type": "lz4",
        "acks": "all",  # money pipeline: never lose a webhook on leader failover
        "enable.idempotence": True,
    }

    producer = SerializingProducer(producer_conf)

    if args.stress > 0:
        logger.info(
            f"Starting STRESS TEST mode. Pushing {args.stress} messages to {settings.topic_name}..."
        )
        start_time = time.time()
        for i in range(args.stress):
            event = generate_webhook_event()
            try:
                producer.produce(
                    topic=settings.topic_name,
                    key=event["transaction_id"],
                    value=event,
                    on_delivery=delivery_report,
                )
            except BufferError:
                logger.warning("Producer queue full, flushing...")
                producer.flush(timeout=30)
                producer.produce(
                    topic=settings.topic_name,
                    key=event["transaction_id"],
                    value=event,
                    on_delivery=delivery_report,
                )
            producer.poll(0)
            if i > 0 and i % 10000 == 0:
                logger.info(f"Pushed {i} messages...")
        producer.flush(timeout=30)
        elapsed = max(time.time() - start_time, 1e-9)
        logger.info(
            f"STRESS TEST COMPLETE: {args.stress} messages in {elapsed:.2f} seconds ({args.stress / elapsed:.2f} msgs/sec)"
        )
        return

    logger.info(f"Starting webhook stream to {settings.topic_name}...")
    try:
        while True:
            event = generate_webhook_event()
            producer.produce(
                topic=settings.topic_name,
                key=event["transaction_id"],
                value=event,
                on_delivery=delivery_report,
            )
            producer.poll(0)
            time.sleep(random.uniform(0.1, 1.5))
    except KeyboardInterrupt:
        logger.info("Stopping producer...")
    finally:
        producer.flush(timeout=30)


if __name__ == "__main__":
    main()
