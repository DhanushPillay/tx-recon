import pytest
from testcontainers.community.kafka import KafkaContainer
from confluent_kafka import Producer, Consumer
import json
import uuid

# We will test basic Kafka produce and consume using testcontainers
# This mimics the integration layer without needing Schema Registry for the basic connectivity test.


@pytest.fixture(scope="module")
def kafka_container():
    with KafkaContainer("confluentinc/cp-kafka:latest") as kafka:
        yield kafka


def test_kafka_producer_integration(kafka_container):
    bootstrap_servers = kafka_container.get_bootstrap_server()
    topic = "test_webhook_topic"

    producer_conf = {
        "bootstrap.servers": bootstrap_servers,
    }
    producer = Producer(producer_conf)

    # Produce a message
    test_event = {
        "transaction_id": f"tx_{uuid.uuid4().hex[:12]}",
        "amount_paise": 50000,
        "gateway_status": "SUCCESS",
    }

    producer.produce(
        topic, key=test_event["transaction_id"], value=json.dumps(test_event)
    )
    producer.flush()

    # Consume the message to verify
    consumer_conf = {
        "bootstrap.servers": bootstrap_servers,
        "group.id": "test_group",
        "auto.offset.reset": "earliest",
    }

    consumer = Consumer(consumer_conf)
    consumer.subscribe([topic])

    msg = consumer.poll(10.0)  # wait up to 10 seconds
    assert msg is not None
    assert not msg.error()

    received_val = json.loads(msg.value().decode("utf-8"))
    assert received_val["transaction_id"] == test_event["transaction_id"]
    assert received_val["amount_paise"] == test_event["amount_paise"]

    consumer.close()
