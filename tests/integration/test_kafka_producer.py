import pytest
from confluent_kafka import Producer, Consumer
import json
import uuid

# Integration tests require Docker. Skip in CI where containers are unreliable.
# Run locally with: pytest tests/integration/ -m integration
pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def kafka_container():
    try:
        from testcontainers.community.kafka import KafkaContainer
    except ImportError:
        pytest.skip("testcontainers not installed")

    try:
        with KafkaContainer("confluentinc/confluent-local:7.4.2") as kafka:
            yield kafka
    except Exception as e:
        pytest.skip(f"Kafka container failed to start: {e}")


def test_kafka_producer_integration(kafka_container):
    bootstrap_servers = kafka_container.get_bootstrap_server()
    topic = "test_webhook_topic"

    producer_conf = {
        "bootstrap.servers": bootstrap_servers,
    }
    producer = Producer(producer_conf)

    test_event = {
        "transaction_id": f"tx_{uuid.uuid4().hex[:12]}",
        "amount_paise": 50000,
        "gateway_status": "SUCCESS",
    }

    producer.produce(
        topic, key=test_event["transaction_id"], value=json.dumps(test_event)
    )
    producer.flush()

    consumer_conf = {
        "bootstrap.servers": bootstrap_servers,
        "group.id": "test_group",
        "auto.offset.reset": "earliest",
    }

    consumer = Consumer(consumer_conf)
    consumer.subscribe([topic])

    msg = consumer.poll(10.0)
    assert msg is not None
    assert not msg.error()

    received_val = json.loads(msg.value().decode("utf-8"))
    assert received_val["transaction_id"] == test_event["transaction_id"]
    assert received_val["amount_paise"] == test_event["amount_paise"]

    consumer.close()
