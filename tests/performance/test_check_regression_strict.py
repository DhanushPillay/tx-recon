"""The regression gate used to warn on a missing metric and exit 0.

CI passes an accuracy-only artifact, so throughput and p99 were never measured
and the step still reported success. These tests pin the new behaviour: a
required metric the run does not supply fails, and an unusable baseline value
cannot read as "no change".
"""

import check_regression as cr


def _kafka(tp=1000.0, p95=10.0, p99=50.0, count=None):
    out = {"throughput_msgs_sec": tp, "ack_latency": {"p95": p95, "p99": p99}}
    if count is not None:
        out["config"] = {"count": count}
    return {"kafka": out}


class TestRequiredFamilies:
    def test_required_family_absent_from_the_run_fails(self):
        cur = {"accuracy": {"match_rate": 0.94}}
        base = {"kafka": {"throughput_msgs_sec": 1000.0}}
        failures, _, _ = cr.check(cur, base, require=("kafka",))
        assert any("--require kafka" in f for f in failures)

    def test_required_family_present_passes(self):
        run = _kafka()
        failures, _, _ = cr.check(run, run, require=("kafka", "kafka-latency"))
        assert failures == []

    def test_unknown_family_is_reported(self):
        failures, _, _ = cr.check({}, {}, require=("nonsense",))
        assert any("unknown metric family" in f for f in failures)

    def test_nothing_required_keeps_the_old_permissive_path(self):
        # Back-compat for callers that pass no scope: absence still only warns.
        base = {"kafka": {"throughput_msgs_sec": 1000.0}}
        failures, warn, compared = cr.check({"accuracy": {"match_rate": 0.94}}, base)
        assert failures == []
        assert warn and compared == set()


class TestPctChange:
    def test_zero_baseline_with_a_real_current_value_is_uncomparable(self):
        assert cr._pct_change(100.0, 0) is None

    def test_both_zero_is_no_change(self):
        assert cr._pct_change(0, 0) == 0.0

    def test_zero_baseline_becomes_a_failure_not_a_silent_zero(self):
        cur = _kafka(tp=500.0)
        base = _kafka(tp=0)
        failures, _, _ = cr.check(cur, base)
        assert any("cannot compute a change" in f for f in failures)

    def test_normal_change_still_computes(self):
        assert cr._pct_change(800.0, 1000.0) == -20.0


class TestP95IsGated:
    def test_p95_rise_fails(self):
        cur, base = _kafka(p95=10.0), _kafka(p95=5.0)
        failures, _, _ = cr.check(cur, base)
        assert any("kafka p95 rose" in f for f in failures)

    def test_p95_within_tolerance_passes(self):
        cur, base = _kafka(p95=11.0), _kafka(p95=10.0)
        failures, _, _ = cr.check(cur, base)
        assert not any("p95" in f for f in failures)

    def test_p95_shows_up_in_the_comparable_set(self):
        names = cr._comparable(_kafka())
        assert "kafka.ack_latency.p95" in names


class TestWorkloadComparability:
    def test_kafka_message_count_change_fails_when_kafka_is_required(self):
        cur, base = _kafka(count=2_000_000), _kafka(count=1_000_000)
        failures, _, _ = cr.check(cur, base, require=("kafka",))
        assert any("message count changed" in f for f in failures)

    def test_missing_iceberg_benchmark_fails_when_iceberg_is_required(self):
        base = {
            "iceberg": {"benchmarks": {"1000k_rows_10pct_update_mor_real": {"rows_per_sec": 100.0}}}
        }
        failures, _, _ = cr.check({"accuracy": {"match_rate": 0.94}}, base, require=("iceberg",))
        assert any("does not" in f for f in failures)


class TestIcebergHealth:
    def test_healthy_false_fails(self):
        cur = {
            "iceberg": {
                "benchmarks": {"b": {"healthy": False, "match_rate": 0.94, "rows_per_sec": 1.0}}
            }
        }
        failures, _, _ = cr.check(cur, cur)
        assert any("healthy=false" in f for f in failures)

    def test_missing_match_rate_fails(self):
        cur = {"iceberg": {"benchmarks": {"b": {"healthy": True, "rows_per_sec": 1.0}}}}
        failures, _, _ = cr.check(cur, cur)
        assert any("no match_rate" in f for f in failures)

    def test_low_match_rate_fails(self):
        cur = {
            "iceberg": {
                "benchmarks": {"b": {"healthy": True, "match_rate": 0.5, "rows_per_sec": 1.0}}
            }
        }
        failures, _, _ = cr.check(cur, cur)
        assert any("match_rate" in f for f in failures)


class TestHardwarePolicy:
    def test_mismatch_warns_by_default(self):
        cur = {"hardware": {"fingerprint": "aaa"}}
        base = {"hardware": {"fingerprint": "bbb"}}
        failures, warn, _ = cr.check(cur, base)
        assert failures == []
        assert any("hardware changed" in w for w in warn)

    def test_mismatch_fails_under_the_fail_policy(self):
        cur = {"hardware": {"fingerprint": "aaa"}}
        base = {"hardware": {"fingerprint": "bbb"}}
        failures, _, _ = cr.check(cur, base, hardware_policy="fail")
        assert any("hardware changed" in f for f in failures)


class TestAccuracy:
    def test_accuracy_artifact_without_match_rate_fails_when_required(self):
        failures, _, _ = cr.check({"accuracy": {}}, {}, require=("accuracy",))
        assert any("no match_rate" in f for f in failures)

    def test_low_accuracy_fails(self):
        failures, _, _ = cr.check({"accuracy": {"match_rate": 0.80}}, {})
        assert any("accuracy gate failed" in f for f in failures)

    def test_healthy_accuracy_passes(self):
        failures, _, _ = cr.check({"accuracy": {"match_rate": 0.9463}}, {})
        assert failures == []


class TestParseRequire:
    def test_parses_comma_separated(self):
        assert cr._parse_require("accuracy,kafka") == ["accuracy", "kafka"]

    def test_tolerates_spaces_and_blanks(self):
        assert cr._parse_require(" accuracy , kafka , ") == ["accuracy", "kafka"]

    def test_empty_gives_empty(self):
        assert cr._parse_require("") == []


class TestCheckedInBaseline:
    def test_baseline_declares_every_family_it_measures(self):
        import json
        import os

        path = os.path.join(os.path.dirname(__file__), "baseline_real.json")
        with open(path) as f:
            base = json.load(f)
        measured = cr._comparable(base)
        assert measured, "the checked-in baseline measures nothing"
        assert any(m.startswith("kafka") for m in measured)
        assert any(m.startswith("iceberg") for m in measured)

    def test_accuracy_only_run_against_the_full_baseline_says_so(self):
        import json
        import os

        path = os.path.join(os.path.dirname(__file__), "baseline_real.json")
        with open(path) as f:
            base = json.load(f)
        failures, warn, compared = cr.check(
            {"accuracy": {"match_rate": 0.94}}, base, require=("accuracy",)
        )
        assert failures == []
        assert compared == set()
        assert len(warn) == len(cr._comparable(base))
        assert all("no comparison ran" in w for w in warn)
