import argparse
import json
import os
import platform
import statistics
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer

WARMUP_MESSAGES = 5000
LATENCY_SAMPLE = 1000
TOPIC_NAME = "gateway_webhooks"


def get_hardware_info():
    return {
        "platform": platform.platform(),
        "cpu_count": os.cpu_count(),
        "python_version": platform.python_version(),
        "hostname": platform.node(),
    }


def percentile(data, p):
    if not data:
        return 0
    k = (len(data) - 1) * (p / 100)
    f = int(k)
    c = min(f + 1, len(data) - 1)
    return data[f] + (k - f) * (data[c] - data[f])


def generate_webhook_event(record_size=1024):
    payload = "x" * max(0, record_size - 120)
    return {
        "transaction_id": f"tx_{uuid.uuid4().hex[:12]}",
        "amount_paise": 1000,
        "gateway_status": "SUCCESS",
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "merchant_id": "merch_12345",
        "payload": payload,
    }


def create_producer(broker, acks, compression):
    conf = {
        "bootstrap.servers": broker,
        "acks": acks,
        "linger.ms": 20,
        "batch.num.messages": 131072,
        "queue.buffering.max.messages": 2000000,
        "queue.buffering.max.kbytes": 524288,
        "compression.type": compression if compression != "none" else "none",
        "message.max.bytes": 1048576,
        "delivery.report.only.error": True,
    }
    return Producer(conf)


def delivery_callback(err, msg):
    pass


def measure_throughput(broker, count, acks, compression, record_size):
    producer = create_producer(broker, acks, compression)

    for _ in range(WARMUP_MESSAGES):
        event = generate_webhook_event(record_size)
        producer.produce(
            TOPIC_NAME,
            key=event["transaction_id"].encode(),
            value=json.dumps(event).encode(),
            callback=delivery_callback,
        )
        producer.poll(0)
    producer.flush(timeout=30)

    start = time.perf_counter()
    for i in range(count):
        event = generate_webhook_event(record_size)
        producer.produce(
            TOPIC_NAME,
            key=event["transaction_id"].encode(),
            value=json.dumps(event).encode(),
            callback=delivery_callback,
        )
        if i % 10000 == 0:
            producer.poll(0)
    producer.flush(timeout=60)
    elapsed = time.perf_counter() - start

    return count / elapsed if elapsed > 0 else 0


def measure_latency(broker, count, acks, compression, record_size):
    producer = create_producer(broker, acks, compression)

    for _ in range(min(WARMUP_MESSAGES, count)):
        event = generate_webhook_event(record_size)
        producer.produce(
            TOPIC_NAME,
            key=event["transaction_id"].encode(),
            value=json.dumps(event).encode(),
        )
        producer.flush(timeout=10)

    latencies_ms = []
    for _ in range(count):
        event = generate_webhook_event(record_size)
        start = time.perf_counter()
        producer.produce(
            TOPIC_NAME,
            key=event["transaction_id"].encode(),
            value=json.dumps(event).encode(),
        )
        producer.flush(timeout=10)
        latencies_ms.append((time.perf_counter() - start) * 1000)

    latencies_ms.sort()
    return {
        "p50": round(percentile(latencies_ms, 50), 2),
        "p95": round(percentile(latencies_ms, 95), 2),
        "p99": round(percentile(latencies_ms, 99), 2),
        "max": round(max(latencies_ms), 2),
        "mean": round(statistics.mean(latencies_ms), 2),
        "stdev": (round(statistics.stdev(latencies_ms), 2) if len(latencies_ms) > 1 else 0),
    }


def run(broker, count, acks, compression, record_size, mode):
    hw = get_hardware_info()
    print(f"Hardware: {hw['platform']}, {hw['cpu_count']} cores, Python {hw['python_version']}")
    print(f"Broker: {broker}, acks={acks}, compression={compression}, record_size={record_size}")

    throughput = None
    latency = None

    if mode in ("throughput", "both"):
        print(f"\n[Throughput] {count:,} messages (async)...")
        rate = measure_throughput(broker, count, acks, compression, record_size)
        throughput = round(rate, 2)
        print(f"  {throughput:,.0f} msgs/sec")

    if mode in ("latency", "both"):
        latency_count = min(LATENCY_SAMPLE, count)
        print(f"\n[Latency] {latency_count:,} messages (serial)...")
        latency = measure_latency(broker, latency_count, acks, compression, record_size)
        print(f"  p50={latency['p50']}ms p95={latency['p95']}ms p99={latency['p99']}ms")

    return {
        "broker": broker,
        "throughput_msgs_sec": throughput,
        "ack_latency": latency,
        "config": {
            "acks": acks,
            "compression": compression,
            "record_size": record_size,
            "count": count,
        },
    }


def main():
    parser = argparse.ArgumentParser(description="Kafka Producer Benchmark")
    parser.add_argument("--count", type=int, default=1000000)
    parser.add_argument("--mode", choices=["throughput", "latency", "both"], default="both")
    parser.add_argument("--acks", choices=["0", "1", "all"], default="1")
    parser.add_argument("--compression", choices=["lz4", "gzip", "snappy", "none"], default="lz4")
    parser.add_argument("--record-size", type=int, default=1024)
    parser.add_argument(
        "--compare",
        action="store_true",
        help="Run against both Kafka:9092 and Redpanda:19092",
    )
    args = parser.parse_args()

    brokers = ["localhost:9092", "localhost:19092"] if args.compare else ["localhost:19092"]

    results = {}
    for broker in brokers:
        label = "kafka" if "9092" in broker and "19" not in broker else "redpanda"
        print(f"\n{'='*50}")
        print(f"  {label.upper()} ({broker})")
        print(f"{'='*50}")
        try:
            results[label] = run(
                broker,
                args.count,
                args.acks,
                args.compression,
                args.record_size,
                args.mode,
            )
        except Exception as e:  # noqa: BLE001
            results[label] = {"error": str(e)}
            print(f"  FAILED: {e}")

    if args.compare and len(results) == 2:
        print(f"\n{'='*60}")
        print("  COMPARISON")
        print(f"{'='*60}")
        print(f"{'Metric':<30} {'Kafka:9092':<15} {'Redpanda:19092':<15}")
        print(f"{'-'*30} {'-'*15} {'-'*15}")
        kafka_r = results.get("kafka", {})
        rp_r = results.get("redpanda", {})
        t_k = kafka_r.get("throughput_msgs_sec") or "error"
        t_r = rp_r.get("throughput_msgs_sec") or "error"
        print(f"{'Throughput (msgs/sec)':<30} {t_k!s:<15} {t_r!s:<15}")
        lat_k = kafka_r.get("ack_latency", {})
        lat_r = rp_r.get("ack_latency", {})
        print(f"{'p50 (ms)':<30} {lat_k.get('p50','error'):<15} {lat_r.get('p50','error'):<15}")
        print(f"{'p99 (ms)':<30} {lat_k.get('p99','error'):<15} {lat_r.get('p99','error'):<15}")

    return results


if __name__ == "__main__":
    main()
