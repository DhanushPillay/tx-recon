from check_regression import check


def test_no_regression_on_identical_runs():
    base = {"kafka": {"throughput_msgs_sec": 1000.0, "ack_latency": {"p99": 50.0}}}
    failures, warn = check(base, base)
    assert failures == []
    assert warn == []


def test_throughput_drop_fails():
    cur = {"kafka": {"throughput_msgs_sec": 800.0}}
    base = {"kafka": {"throughput_msgs_sec": 1000.0}}
    failures, _ = check(cur, base)
    assert len(failures) == 1


def test_hardware_change_warns_not_fails():
    cur = {"kafka": {}, "hardware": {"fingerprint": "aaa"}}
    base = {"kafka": {}, "hardware": {"fingerprint": "bbb"}}
    failures, warn = check(cur, base)
    assert failures == []
    assert len(warn) == 1
