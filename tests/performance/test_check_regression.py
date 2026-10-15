from check_regression import _comparable, check


def test_no_regression_on_identical_runs():
    base = {"kafka": {"throughput_msgs_sec": 1000.0, "ack_latency": {"p99": 50.0}}}
    failures, warn, compared = check(base, base)
    assert failures == []
    assert warn == []
    assert compared == {"kafka.throughput_msgs_sec", "kafka.ack_latency.p99"}


def test_throughput_drop_fails():
    cur = {"kafka": {"throughput_msgs_sec": 800.0}}
    base = {"kafka": {"throughput_msgs_sec": 1000.0}}
    failures, _, _ = check(cur, base)
    assert len(failures) == 1


def test_hardware_change_warns_not_fails():
    cur = {"kafka": {}, "hardware": {"fingerprint": "aaa"}}
    base = {"kafka": {}, "hardware": {"fingerprint": "bbb"}}
    failures, warn, _ = check(cur, base)
    assert failures == []
    assert len(warn) == 1


def test_checked_in_baseline_gates_the_mix_only():
    import json
    import os

    with open(os.path.join(os.path.dirname(__file__), "baseline_real.json")) as f:
        base = json.load(f)
    assert base["mix"]["match_rate"] >= 0.85
    ok, warn, compared = check({"mix": {"match_rate": 0.94}}, base)
    assert ok == []
    assert compared == set()
    assert len(warn) == len(_comparable(base))
    assert all("no comparison ran" in w for w in warn)
    bad, _, _ = check({"mix": {"match_rate": 0.80}}, base)
    assert any("mix floor" in f for f in bad)
