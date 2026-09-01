# Kafka Producer Benchmark Explained

> **Environment**: Single-node Docker, localhost only. Results do not represent multi-node Kafka/Redpanda clusters. Run on your own hardware to validate.

The Kafka producer benchmark measures local throughput from a single producer to a single Redpanda broker.

## The Short Answer

We switched from `kafka-python-ng` (pure Python) to `confluent-kafka` (C/librdkafka-backed), added lz4 compression, and tuned batching. That's it.

## What Changed

| Aspect | Before | After |
|---|---|---|
| Library | `kafka-python-ng` (pure Python) | `confluent-kafka` (C/librdkafka) |
| Compression | None | lz4 |
| Buffer memory | 32 MB | 2 GB |
| Batch size | ~16K messages | 131,072 messages |
| Linger time | ~0 ms (send immediately) | 20 ms (batch before flush) |
| Max queued msgs | default | 2,000,000 |
| Poll frequency | every message | every 10,000 messages |
| Callbacks | success + failure per message | failure-only |

## Why Each Change Matters

### 1. The library swap (the big one)

`kafka-python-ng` is a pure Python Kafka client. Every message goes through Python's GIL — serialization, batching decisions, network I/O all happen in a single thread.

`confluent-kafka` wraps librdkafka, a battle-tested C library. The heavy lifting (protocol handling, compression, batching) happens in compiled C code, bypassing the GIL entirely. This alone accounts for most of the 5-7x improvement.

### 2. Compression (lz4)

The original benchmark had compression off. Adding `lz4` compresses payloads on the wire — less data to transfer per message, faster throughput. lz4 is chosen for speed over ratio (gzip compresses more but is slower).

### 3. Aggressive batching

Default Kafka producers send messages as fast as possible — which means many small network round-trips. Tuning these parameters batches messages together:

- **`linger.ms=20`**: Wait up to 20ms to accumulate messages before sending a batch. This trades a tiny bit of latency for much higher throughput.
- **`batch.num.messages=131072`**: Up to 128K messages per batch per network request.
- **`queue.buffering.max.messages=2000000`**: 2M message buffer so the producer never blocks waiting for the broker.

### 4. Larger memory buffer

32 MB → 2 GB. The producer can hold millions of messages in memory before blocking. This keeps the pipeline full even if the broker is momentarily busy.

### 5. Fewer callbacks

`delivery.report.only.error=True` tells the client to skip success callbacks entirely. In the old benchmark, every message triggered a callback on completion. With millions of messages, callback overhead adds up. We only care about failures.

### 6. Less frequent polling

Polling every 10,000 messages instead of every message reduces Python-to-C boundary crossings. Each poll is a context switch — fewer polls = less overhead.

## Benchmark Configuration

The final producer config (`kafka_producer_benchmark.py`):

```python
conf = {
    "bootstrap.servers": "localhost:19092",
    "acks": "1",
    "linger.ms": 20,
    "batch.num.messages": 131072,
    "queue.buffering.max.messages": 2000000,
    "queue.buffering.max.kbytes": 524288,  # 512 KB per partition
    "compression.type": "lz4",
    "message.max.bytes": 1048576,  # 1 MB max message size
    "delivery.report.only.error": True,
}
```

Run it yourself:

```bash
# Throughput only (fast)
python tests/performance/kafka_producer_benchmark.py --count 1000000 --mode throughput

# Both throughput + latency
python tests/performance/kafka_producer_benchmark.py --count 1000000 --mode both

# Compare Kafka vs Redpanda
python tests/performance/kafka_producer_benchmark.py --count 1000000 --compare
```

## What We Didn't Change

- **Broker**: Still Redpanda v23.2.2 (Kafka-compatible)
- **Network**: Same localhost connection
- **Message format**: Same JSON webhook payload
- **Key distribution**: Same UUID-based keys

The improvement is purely client-side tuning.
