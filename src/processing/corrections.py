import json
import logging
import os
import time

logger = logging.getLogger(__name__)


def journal_correction(
    data_dir: str,
    action: str,
    reason: str,
    approved_by: str,
    details: dict | None = None,
) -> str:
    if not (reason or "").strip():
        raise ValueError("correction requires a reason (ticket/cause)")
    if not (approved_by or "").strip():
        raise ValueError("correction requires approved_by (second pair of eyes)")
    entry = {
        "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "action": action,
        "reason": reason.strip(),
        "approved_by": approved_by.strip(),
        "details": details or {},
    }
    path = os.path.join(data_dir, "corrections.jsonl")
    try:
        with open(path, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(entry) + "\n")
    except OSError as exc:
        raise RuntimeError(f"cannot write corrections journal {path}: {exc}") from exc
    logger.warning("correction journaled: %s by %s (%s)", action, approved_by, reason)
    return path
