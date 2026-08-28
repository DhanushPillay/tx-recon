import json
import time
import random
import uuid
from datetime import datetime, timezone
from confluent_kafka import Producer

# Configuration for local Redpanda/Kafka
KAFKA_BROKER = "localhost:19092"
TOPIC_NAME = "gateway_webhooks"

def generate_webhook_event():
    # Random gateway amounts between ₹10 and ₹10,000
    amount_paise = random.randint(1000, 1000000)
    tx_id = f"tx_{uuid.uuid4().hex[:12]}"
    
    event = {
        "transaction_id": tx_id,
        "amount_paise": amount_paise, # Keep currency as integers to avoid floating point math errors
        "gateway_status": "SUCCESS",
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "merchant_id": "merch_12345"
    }
    return event

def receipt(err, msg):
    if err:
        print(f"Error producing message: {err}")
    else:
        print(f"Produced message to {msg.topic()} partition [{msg.partition()}] @ offset {msg.offset()}")

def main():
    conf = {
        'bootstrap.servers': KAFKA_BROKER,
        'client.id': 'python-producer'
    }
    
    producer = Producer(conf)
    
    print(f"Starting webhook stream to {TOPIC_NAME}...")
    try:
        while True:
            event = generate_webhook_event()
            # Serialize JSON
            payload = json.dumps(event)
            
            # Produce to Redpanda
            producer.produce(TOPIC_NAME, key=event["transaction_id"], value=payload, callback=receipt)
            producer.poll(0) # Serve delivery reports
            
            # Streaming cadence
            time.sleep(random.uniform(0.1, 1.5))
            
    except KeyboardInterrupt:
        print("Stopping producer...")
    finally:
        producer.flush()

if __name__ == "__main__":
    main()
