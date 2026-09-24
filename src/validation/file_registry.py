"""Processed-file registry: idempotency on content, not delivery.

Same bytes under a different name (or a redelivered drop) share a sha256,
so re-ingest is a deliberate idempotent re-merge, never a double count.
Registry is a small JSON sidecar next to the data dir (ops-visible, auditable).
"""

import hashlib
import json
import logging
import os
import time

logger = logging.getLogger(__name__)

REGISTRY_NAME = ".processed_files.json"


def file_hash(path: str, chunk_size: int = 1 << 20) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(chunk_size), b""):
            h.update(chunk)
    return h.hexdigest()


def _registry_path(data_dir: str) -> str:
    return os.path.join(data_dir, REGISTRY_NAME)


def load_registry(data_dir: str) -> dict:
    try:
        with open(_registry_path(data_dir), encoding="utf-8") as fh:
            return json.load(fh)
    except (OSError, ValueError):
        return {}


def record_file(data_dir: str, path: str, batch_date: str | None) -> dict:
    """Record a processed file. Returns {sha256, seen_before}."""
    digest = file_hash(path)
    reg = load_registry(data_dir)
    seen_before = digest in reg
    reg[digest] = {
        "name": os.path.basename(path),
        "batch_date": batch_date,
        "recorded_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    try:
        with open(_registry_path(data_dir), "w", encoding="utf-8") as fh:
            json.dump(reg, fh, indent=2)
    except OSError as exc:
        logger.warning(f"file registry write skipped: {exc}")
    if seen_before:
        logger.warning(
            f"redelivery: {os.path.basename(path)} matches known sha256 "
            f"(first seen as {reg[digest]['name']}) — idempotent re-merge"
        )
    return {"sha256": digest, "seen_before": seen_before}
