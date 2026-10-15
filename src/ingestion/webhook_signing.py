"""Signature verification for signed gateway events.

This project owns no HTTP listener. Webhooks arrive on a Kafka topic, Spark's
Kafka source does not expose record headers, and the Avro payload carries no
signature field. So there is nowhere in the ingestion path where a signature
could be checked even in principle. ``SECURITY.md`` said so before this module
existed, and setting ``WEBHOOK_SECRET`` still authenticates nothing on its own.

What this is for: an ingress adapter or a gateway edge outside this repository
that does receive HTTP, has the secret, and needs to know whether to trust an
event before it puts one on the topic. It is a library, not a server, on
purpose. Nothing here listens, and nothing here is wired into
``ingest_webhooks.py``.

The scheme is the one Stripe-style gateways use and the one most HMAC guides
describe:

    header:  x-tx-sig: t=1752000000,v1=<lowercase hex>
    signed:  f"{t}.{raw_body_bytes}"

Four things that are easy to skip and that this does not:

* the signature covers the timestamp, so a captured event cannot be replayed
  later with its original signature;
* the timestamp is bounded in both directions, so a valid signature on an old
  body is refused;
* comparison is ``hmac.compare_digest`` on the decoded bytes, so a caller
  cannot learn the digest one byte at a time;
* a delivery id is remembered for the length of the freshness window, so the
  same valid event sent twice is a replay rather than a silent double count.

Nothing is logged here. Callers decide what to record, and an event id is the
only identifier this module ever sees.
"""

from __future__ import annotations

import hmac
import time
from collections import OrderedDict
from dataclasses import dataclass
from enum import StrEnum

#: How far a timestamp may sit from now, in seconds. Five minutes is the usual
#: allowance for clock skew plus network delay; anything larger widens the
#: window in which a captured event is still good.
DEFAULT_TOLERANCE_SEC = 300

#: Header the gateway edge is expected to set. Named here so the contract has
#: one definition rather than one per document.
SIGNATURE_HEADER = "x-tx-sig"

#: Header carrying the per-delivery id used for replay detection. Separate from
#: the signature because the id is not covered by it: a replay resends the
#: original bytes, original signature and all.
DELIVERY_ID_HEADER = "x-tx-id"

#: Version tag carried in the header, so the scheme can change later without a
#: signature written under the old one silently failing to verify.
VERSION = "v1"


class Reason(StrEnum):
    """Why verification did or did not succeed.

    A reason rather than a bool because every one of these needs a different
    response. An expired signature is a retryable clock problem; a bad one is
    a forgery; a replay is a duplicate that must not be counted twice. Callers
    that collapse them to "reject" lose the ability to tell an operator what
    happened.
    """

    VALID = "valid"
    NOT_CONFIGURED = "not_configured"
    MISSING_HEADER = "missing_header"
    MALFORMED_HEADER = "malformed_header"
    UNKNOWN_VERSION = "unknown_version"
    BAD_SIGNATURE = "bad_signature"
    EXPIRED = "expired"
    REPLAYED = "replayed"
    MISSING_DELIVERY_ID = "missing_delivery_id"


@dataclass(frozen=True)
class Result:
    ok: bool
    reason: Reason
    timestamp: int | None = None

    def __bool__(self) -> bool:
        return self.ok


class ReplayCache:
    """Delivery ids seen recently, bounded by count and by age.

    Both bounds matter. Count alone lets an attacker who can forge nothing still
    push out the real ids by flooding; age alone lets a long-running process
    accumulate one entry per delivery forever. Oldest-first eviction is the
    weaker guarantee in both cases, which is the right way round here: a flood
    can displace a recent id and get that one event reprocessed, but cannot
    resurrect an old signature.
    """

    def __init__(self, max_entries: int = 10_000, ttl_sec: int = DEFAULT_TOLERANCE_SEC * 2):
        if max_entries < 1:
            raise ValueError("max_entries must be at least 1")
        self.max_entries = max_entries
        self.ttl_sec = ttl_sec
        self._seen: OrderedDict[str, float] = OrderedDict()

    def __len__(self) -> int:
        self._expire()
        return len(self._seen)

    def __contains__(self, delivery_id: str) -> bool:
        return self.seen(delivery_id)

    def _expire(self, now: float | None = None) -> None:
        cutoff = (time.time() if now is None else now) - self.ttl_sec
        while self._seen:
            oldest = next(iter(self._seen))
            if self._seen[oldest] > cutoff:
                break
            self._seen.popitem(last=False)

    def seen(self, delivery_id: str, now: float | None = None) -> bool:
        """Has this id arrived recently? Does not record it."""
        self._expire(now)
        return delivery_id in self._seen

    def remember(self, delivery_id: str, now: float | None = None) -> None:
        self._expire(now)
        self._seen[delivery_id] = time.time() if now is None else now
        self._seen.move_to_end(delivery_id)
        while len(self._seen) > self.max_entries:
            self._seen.popitem(last=False)


def sign(secret: str | bytes, timestamp: int | str, body: str | bytes) -> str:
    """Produce the header value a gateway would send.

    Symmetric with :func:`verify` by construction: both build the same signed
    string through :func:`_signed_payload`, so a change to one cannot silently
    diverge from the other.
    """
    payload = _signed_payload(timestamp, body)
    return f"t={timestamp},v1={_digest_hex(_key(secret), payload)}"


def _key(secret: str | bytes) -> bytes:
    return secret.encode() if isinstance(secret, str) else secret


def _body_bytes(body: str | bytes) -> bytes:
    return body.encode() if isinstance(body, str) else body


def _signed_payload(timestamp: int | str, body: str | bytes) -> bytes:
    # Separator matters: without a delimiter that cannot appear in the
    # timestamp, ("12", "3456") and ("123", "456") would sign identically.
    return b"%s.%s" % (_body_bytes(str(timestamp)), _body_bytes(body))


def _digest_hex(key: bytes, payload: bytes) -> str:
    import hashlib

    return hmac.new(key, payload, hashlib.sha256).hexdigest()


def _parse_header(header: str) -> tuple[int, dict[str, str]]:
    """Pull the timestamp and versioned digests out of ``t=..,v1=..``.

    Raises ValueError on anything malformed; the caller turns that into a
    MALFORMED_HEADER result rather than an exception, because a malformed header
    is bad input, not a broken program.
    """
    parts: dict[str, str] = {}
    timestamp: int | None = None
    for chunk in header.split(","):
        chunk = chunk.strip()
        if not chunk or "=" not in chunk:
            raise ValueError(f"expected key=value, got {chunk!r}")
        key, _, value = chunk.partition("=")
        key = key.strip()
        if key == "t":
            timestamp = int(value.strip())
        else:
            parts[key.strip()] = value.strip()
    if timestamp is None:
        raise ValueError("no t= component")
    if not parts:
        raise ValueError("no signature component")
    return timestamp, parts


def verify(
    secret: str | bytes | None,
    header: str | None,
    body: str | bytes,
    *,
    delivery_id: str | None = None,
    now: int | float | None = None,
    tolerance_sec: int = DEFAULT_TOLERANCE_SEC,
    replay_cache: ReplayCache | None = None,
) -> Result:
    """Check one event. Never raises for bad input; returns a Result.

    ``now`` and ``replay_cache`` exist so the tests can place an event in time
    without sleeping. Production callers pass neither.
    """
    if not secret:
        # Not a rejection of this event: a refusal to make any claim about it.
        # Silently passing here would be the exact failure this whole module is
        # about, so it is a distinct reason and it is not ok.
        return Result(False, Reason.NOT_CONFIGURED)
    if header is None or header == b"" or header == "":
        return Result(False, Reason.MISSING_HEADER)
    if not delivery_id:
        return Result(False, Reason.MISSING_DELIVERY_ID)

    # WSGI servers hand headers over as bytes; callers reading them off a
    # request object usually have str. Both have to work.
    if isinstance(header, (bytes, bytearray)):
        try:
            header = header.decode("ascii")
        except UnicodeDecodeError:
            return Result(False, Reason.MALFORMED_HEADER)

    try:
        timestamp, versions = _parse_header(header)
    except (ValueError, TypeError):
        return Result(False, Reason.MALFORMED_HEADER)

    supplied = versions.get(VERSION)
    if supplied is None:
        return Result(False, Reason.UNKNOWN_VERSION, timestamp)

    # A digest that is not hex is a malformed header, not a wrong one, and the
    # difference matters to whoever reads the log.
    if not _is_hex(supplied):
        return Result(False, Reason.MALFORMED_HEADER, timestamp)

    current = time.time() if now is None else now
    if abs(current - timestamp) > tolerance_sec:
        return Result(False, Reason.EXPIRED, timestamp)

    if replay_cache is not None and replay_cache.seen(delivery_id, current):
        return Result(False, Reason.REPLAYED, timestamp)

    # Compare the decoded bytes, not the hex strings, and on every digest the
    # header offered rather than stopping at the first mismatch, so the work
    # done does not depend on which version matched.
    key = _key(secret)
    payload = _signed_payload(timestamp, body)
    import hashlib

    expected = hmac.new(key, payload, hashlib.sha256).digest()
    offered = [bytes.fromhex(v) for v in versions.values() if _is_hex(v)]
    matched = any(hmac.compare_digest(expected, candidate) for candidate in offered)

    if not matched:
        return Result(False, Reason.BAD_SIGNATURE, timestamp)
    if replay_cache is not None:
        replay_cache.remember(delivery_id, current)
    return Result(True, Reason.VALID, timestamp)


def _is_hex(value: str) -> bool:
    try:
        bytes.fromhex(value)
    except ValueError:
        return False
    return True
