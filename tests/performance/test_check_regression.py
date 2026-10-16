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


def test_every_required_family_is_one_the_gate_knows():
    """The gate rejects a family it does not know, so somebody has to check the names.

    Renaming the family from `accuracy` to `mix` left the workflow requiring the old
    name, which turned the regression gate into a step that failed on every push and
    nothing caught it. The workflow and the Makefile are the only two places that ask
    for a family by name, so this reads them both.
    """
    import re
    from pathlib import Path

    import check_regression as cr

    root = Path(__file__).resolve().parents[2]
    found = 0
    for name in (".github/workflows/ci.yml", "Makefile"):
        # Comments do not run. A comment that mentions --require describes nothing.
        for line in (root / name).read_text(encoding="utf-8").splitlines():
            if line.lstrip().startswith("#"):
                continue
            for arg in re.findall(r"--require[= ]([A-Za-z0-9_,\-]+)", line):
                for family in arg.split(","):
                    assert family in cr.FAMILIES, (
                        f"{name} requires metric family {family!r}; the gate knows "
                        f"{sorted(cr.FAMILIES)}. A name it does not know fails the step."
                    )
                    found += 1
    assert found >= 3, (
        f"the scan read {found} --require values, so it is not reading the lines that run"
    )
