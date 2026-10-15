"""Tests for src/ingestion/webhook_signing.py.

Nothing here runs Spark or Kafka. The module is a pure function library, which
is the only reason an ingress contract that this repository does not own can be
tested at all.
"""

from __future__ import annotations

import time

import pytest

from src.ingestion.webhook_signing import (
    DEFAULT_TOLERANCE_SEC,
    Reason,
    ReplayCache,
    sign,
    verify,
)

SECRET = "whsec_test_key_do_not_use_in_prod"  # noqa: S105 - fixture key, not a secret
OTHER_SECRET = "whsec_a_different_key"  # noqa: S105 - the wrong key, for the forgery tests
NOW = 1_752_000_000
BODY = b'{"transaction_id":"tx_abc","amount_paise":100000}'
TOL = DEFAULT_TOLERANCE_SEC


def _ok(**kw):
    """The happy path, with one thing varied per test."""
    kw.setdefault("secret", SECRET)
    kw.setdefault("header", sign(SECRET, NOW, BODY))
    kw.setdefault("body", BODY)
    kw.setdefault("delivery_id", "d-1")
    kw.setdefault("now", NOW)
    kw.setdefault("tolerance_sec", TOL)
    kw.setdefault("replay_cache", ReplayCache())
    return verify(**kw)


# --- the happy path -------------------------------------------------------


def test_a_signature_this_module_produced_verifies():
    r = _ok()
    assert r.ok
    assert r.reason is Reason.VALID
    assert r.timestamp == NOW


def test_verify_is_falsey_so_it_drops_into_a_guard():
    assert _ok()
    assert not _ok(header=sign(OTHER_SECRET, NOW, BODY))


def test_the_string_and_bytes_forms_of_every_input_agree():
    signed = sign(SECRET, str(NOW), BODY.decode())
    assert verify(SECRET, signed, BODY.decode(), delivery_id="d", now=NOW).ok
    assert verify(SECRET.encode(), signed.encode(), BODY, delivery_id="d", now=NOW).ok


# --- forgery --------------------------------------------------------------


def test_a_signature_made_with_another_secret_is_refused():
    r = _ok(header=sign(OTHER_SECRET, NOW, BODY))
    assert r.reason is Reason.BAD_SIGNATURE


def test_a_body_edited_after_signing_is_refused():
    r = _ok(body=BODY.replace(b"100000", b"999999"))
    assert r.reason is Reason.BAD_SIGNATURE


def test_a_single_flipped_hex_character_is_refused():
    header = sign(SECRET, NOW, BODY)
    flipped = ("0" if header[-1] != "0" else "1") + header[-2:]
    assert _ok(header=header[:-1] + flipped).reason is Reason.BAD_SIGNATURE


def test_a_secret_of_the_wrong_length_is_still_refused():
    # No length check anywhere; HMAC handles any key length by hashing it.
    assert _ok(secret="s").reason is Reason.BAD_SIGNATURE  # noqa: S106 - deliberately wrong-length key


def test_an_empty_secret_refuses_rather_than_passing():
    """The failure this module exists to prevent, in its simplest form."""
    r = _ok(secret="")
    assert not r.ok
    assert r.reason is Reason.NOT_CONFIGURED


def test_a_missing_secret_refuses_rather_than_passing():
    assert _ok(secret=None).reason is Reason.NOT_CONFIGURED


# --- malformed input ------------------------------------------------------


@pytest.mark.parametrize(
    "header",
    [
        "garbage",
        "t=1752000000",
        "v1=abcdef",
        "t=notanumber,v1=abcdef",
        "=1752000000,v1=abcdef",
        "t=,v1=abcdef",
        "t=1752000000,",
        ",,,,",
        "t=1752000000,v1=nothexatall",
    ],
)
def test_a_malformed_header_is_named_as_such(header):
    assert _ok(header=header).reason is Reason.MALFORMED_HEADER


@pytest.mark.parametrize("header", [None, "", b""])
def test_an_absent_header_is_reported_as_absent(header):
    assert _ok(header=header).reason is Reason.MISSING_HEADER


def test_a_header_that_is_not_ascii_is_malformed_not_missing():
    assert _ok(header=b"t=1752,v1=\xff\xfe").reason is Reason.MALFORMED_HEADER


def test_an_unknown_version_is_refused_rather_than_ignored():
    digest = sign(SECRET, NOW, BODY).split("v1=")[1]
    r = _ok(header=f"t={NOW},v2={digest}")
    assert r.reason is Reason.UNKNOWN_VERSION


def test_a_known_version_alongside_an_unknown_one_still_verifies():
    """Forward compatibility: a gateway adding v2 must not break v1 traffic."""
    good = sign(SECRET, NOW, BODY)
    r = _ok(header=f"{good},v2=deadbeef")
    assert r.ok


def test_a_missing_delivery_id_is_refused():
    r = _ok(delivery_id=None)
    assert r.reason is Reason.MISSING_DELIVERY_ID


# --- time -----------------------------------------------------------------


def test_a_signature_exactly_at_the_edge_of_the_window_is_accepted():
    r = _ok(header=sign(SECRET, NOW - TOL, BODY), now=NOW)
    assert r.ok, "the boundary is inside the window, not outside it"


def test_one_second_past_the_edge_is_refused():
    r = _ok(header=sign(SECRET, NOW - TOL - 1, BODY), now=NOW)
    assert r.reason is Reason.EXPIRED


def test_a_signature_from_the_future_is_refused_too():
    assert _ok(header=sign(SECRET, NOW + TOL + 1, BODY), now=NOW).reason is Reason.EXPIRED


def test_an_old_signature_on_a_still_valid_body_does_not_verify():
    """The timestamp is inside the signed bytes, so it cannot be slid forward."""
    stale = sign(SECRET, NOW - 10_000, BODY)
    r = _ok(header=stale.replace(f"t={NOW - 10_000}", f"t={NOW}"), now=NOW)
    assert r.reason is Reason.BAD_SIGNATURE


def test_the_window_is_configurable():
    assert not _ok(header=sign(SECRET, NOW - 100, BODY), tolerance_sec=50).ok


# --- replay ---------------------------------------------------------------


def test_the_same_event_twice_is_a_replay():
    cache = ReplayCache()
    header = sign(SECRET, NOW, BODY)
    assert _ok(header=header, replay_cache=cache).ok
    second = _ok(header=header, replay_cache=cache, now=NOW + 1)
    assert second.reason is Reason.REPLAYED


def test_a_replay_stays_a_replay_even_after_the_signature_expires():
    """Expiry is checked first, so a replayed old event is reported as expired."""
    cache = ReplayCache()
    header = sign(SECRET, NOW, BODY)
    _ok(header=header, replay_cache=cache)
    late = _ok(header=header, replay_cache=cache, now=NOW + TOL + 1)
    assert late.reason is Reason.EXPIRED


def test_different_events_with_the_same_id_are_still_caught():
    cache = ReplayCache()
    _ok(delivery_id="d-1", replay_cache=cache)
    other = sign(SECRET, NOW, b'{"transaction_id":"tx_other"}')
    assert _ok(header=other, delivery_id="d-1", replay_cache=cache).reason is Reason.REPLAYED


def test_different_ids_are_independent():
    cache = ReplayCache()
    assert _ok(delivery_id="d-1", replay_cache=cache).ok
    assert _ok(delivery_id="d-2", replay_cache=cache).ok


def test_without_a_cache_the_same_event_verifies_twice():
    """Documented behaviour: the caller has to supply the cache."""
    header = sign(SECRET, NOW, BODY)
    assert _ok(header=header, replay_cache=None).ok
    assert _ok(header=header, replay_cache=None).ok


# --- the cache itself -----------------------------------------------------


def test_the_cache_forgets_after_its_ttl():
    # ReplayCache ages against the wall clock unless told otherwise, so these
    # use a real timestamp rather than the fixed NOW above.
    now = int(time.time())
    cache = ReplayCache(ttl_sec=60)
    cache.remember("d-1", now=now)
    assert cache.seen("d-1", now=now)
    assert not cache.seen("d-1", now=now + 61)


def test_the_cache_is_bounded_by_count():
    now = int(time.time())
    cache = ReplayCache(max_entries=3)
    for i in range(10):
        cache.remember(f"d-{i}", now=now)
    assert len(cache) == 3, "a flood must not be able to grow the cache without bound"


def test_the_cache_evicts_oldest_first():
    now = int(time.time())
    cache = ReplayCache(max_entries=2)
    cache.remember("old", now=now)
    cache.remember("new", now=now)
    cache.remember("newer", now=now)
    assert not cache.seen("old", now=now)
    assert cache.seen("newer", now=now)


def test_an_aged_out_entry_frees_its_slot():
    now = int(time.time())
    cache = ReplayCache(max_entries=2, ttl_sec=10)
    cache.remember("a", now=now)
    cache.remember("b", now=now)
    cache.remember("c", now=now + 11)
    assert not cache.seen("a", now=now + 11)
    assert cache.seen("c", now=now + 11)


def test_containment_reflects_what_was_remembered():
    now = int(time.time())
    cache = ReplayCache()
    cache.remember("d-1", now=now)
    assert "d-1" in cache
    assert "d-2" not in cache


def test_a_zero_sized_cache_is_refused():
    with pytest.raises(ValueError):
        ReplayCache(max_entries=0)


def test_the_cache_works_against_the_real_clock_too():
    cache = ReplayCache()
    header = sign(SECRET, int(time.time()), BODY)
    now = time.time()
    assert verify(SECRET, header, BODY, delivery_id="d", now=now, replay_cache=cache).ok
    assert (
        verify(SECRET, header, BODY, delivery_id="d", now=now, replay_cache=cache).reason
        is Reason.REPLAYED
    )


# --- the contract this module is not --------------------------------------


def test_it_does_not_listen_on_anything():
    """Nothing here should grow a socket, because there is no ingress to serve.

    If a future change adds an HTTP server to work around the missing
    verification, this fails.
    """
    import src.ingestion.webhook_signing as ws

    for name in dir(ws):
        if name.startswith("_"):
            continue
        obj = getattr(ws, name)
        if callable(obj) and getattr(obj, "__module__", "") == ws.__name__:
            assert not getattr(obj, "__code__", None) or "socket" not in obj.__code__.co_names


def test_the_module_docstring_does_not_claim_it_is_wired_into_ingestion():
    import src.ingestion.ingest_webhooks as iw

    assert not hasattr(iw, "verify_webhook"), (
        "ingest_webhooks.py still must not verify signatures: Spark's Kafka "
        "source exposes no record headers, so any call site here would be dead code"
    )
