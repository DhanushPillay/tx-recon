import argparse
import json
import platform
import statistics
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer

KAFKA_BROKER = "localhost:19092"
TOPIC_NAME = "gateway_webhooks"

WARMUP_MESSAGES = 5000
LATENCY_SAMPLE = 1000


def get_hardware_info():
    import os

    return {
        "os": f"{platform.system()} {platform.release()}",
        "cpu": platform.processor() or "unknown",
        "python": platform.python_version(),
        "hostname": platform.node(),
        "cores": os.cpu_count(),
    }


def generate_webhook_event():
    return {
        "transaction_id": f"tx_{uuid.uuid4().hex[:12]}",
        "amount_paise": 1000,
        "gateway_status": "SUCCESS",
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "merchant_id": "merch_12345",
    }


def percentile(data, p):
    if not data:
        return 0
    k = (len(data) - 1) * (p / 100)
    f = int(k)
    c = min(f + 1, len(data) - 1)
    return data[f] + (k - f) * (data[c] - data[f])


def create_producer(acks, linger_ms, batch_size):
    return KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda m: json.dumps(m).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        linger_ms=linger_ms,
        batch_size=batch_size,
        acks=acks,
        max_in_flight_requests_per_connection=5,
        buffer_memory=33554432,
        compression_type=None,
    )


def measure_throughput(count, acks, linger_ms, batch_size):
    """Async sends + flush at end. Measures max throughput."""
    producer = create_producer(acks, linger_ms, batch_size)

    # Warmup
    for _ in range(WARMUP_MESSAGES):
        event = generate_webhook_event()
        producer.send(TOPIC_NAME, key=event["transaction_id"], value=event)
    producer.flush()

    # Measure
    start = time.time()
    for _ in range(count):
        event = generate_webhook_event()
        producer.send(TOPIC_NAME, key=event["transaction_id"], value=event)
    producer.flush()
    elapsed = time.time() - start

    producer.close()
    return count / elapsed if elapsed > 0 else 0


def measure_latency(count, acks, linger_ms, batch_size):
    """Serial sends with .get(). Measures per-message ack latency."""
    producer = create_producer(acks, linger_ms, batch_size)

    # Warmup
    for _ in range(min(WARMUP_MESSAGES, count)):
        event = generate_webhook_event()
        future = producer.send(TOPIC_NAME, key=event["transaction_id"], value=event)
        future.get(timeout=30)

    # Measure
    latencies_ms = []
    for _ in range(count):
        event = generate_webhook_event()
        start = time.time()
        future = producer.send(TOPIC_NAME, key=event["transaction_id"], value=event)
        future.get(timeout=30)
        latencies_ms.append((time.time() - start) * 1000)

    producer.close()
    latencies_ms.sort()
    return {
        "p50": percentile(latencies_ms, 50),
        "p95": percentile(latencies_ms, 95),
        "p99": percentile(latencies_ms, 99),
        "max": max(latencies_ms),
        "mean": statistics.mean(latencies_ms),
        "stdev": statistics.stdev(latencies_ms) if len(latencies_ms) > 1 else 0,
    }


def run(count, acks, linger_ms, batch_size):
    hw = get_hardware_info()
    print(f"Hardware: {hw['os']}, {hw['cores']} cores, Python {hw['python']}")
    print(f"Config: acks={acks}, linger_ms={linger_ms}, batch_size={batch_size}")

    # Throughput test (async)
    print(f"\n[1/2] Throughput test ({count:,} messages, async)...")
    throughput = measure_throughput(count, acks, linger_ms, batch_size)
    print(f"  Throughput: {throughput:,.0f} msgs/sec")

    # Latency test (serial, smaller sample)
    latency_count = min(LATENCY_SAMPLE, count)
    print(f"\n[2/2] Latency test ({latency_count:,} messages, serial)...")
    latency = measure_latency(latency_count, acks, linger_ms, batch_size)
    print(
        f"  Ack latency: p50={latency['p50']:.2f}ms, "
        f"p95={latency['p95']:.2f}ms, "
        f"p99={latency['p99']:.2f}ms, "
        f"max={latency['max']:.2f}ms"
    )
    print(f"  Mean={latency['mean']:.2f}ms, stdev={latency['stdev']:.2f}ms")

    print(f"\n{'='*50}")
    print(f"Throughput:  {throughput:,.0f} msgs/sec (acks={acks})")
    print(
        f"Ack latency: p50={latency['p50']:.2f}ms, p95={latency['p95']:.2f}ms, "
        f"p99={latency['p99']:.2f}ms"
    )

    return {
        "throughput_msgs_sec": round(throughput, 2),
        "ack_latency": {k: round(v, 2) for k, v in latency.items()},
        "hardware": hw,
        "config": {"acks": acks, "linger_ms": linger_ms, "batch_size": batch_size},
    }


def main():
    parser = argparse.ArgumentParser(description="Kafka Producer Benchmark")
    parser.add_argument("--count", type=int, default=100000)
    parser.add_argument("--acks", choices=["0", "1", "all"], default="all")
    parser.add_argument("--linger-ms", type=int, default=5)
    parser.add_argument("--batch-size", type=int, default=65536)
    args = parser.parse_args()

    run(args.count, args.acks, args.linger_ms, args.batch_size)


if __name__ == "__main__":
    main()
