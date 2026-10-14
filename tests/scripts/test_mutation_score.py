"""Unit tests for scripts/mutation_score.py.

mutmut needs WSL and hours per run, so the gate's arithmetic is tested here
against synthetic stats instead. The shapes below match what
`mutmut export-cicd-stats` writes (mutmut/__main__.py: save_cicd_stats).
"""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest

_SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "mutation_score.py"
_spec = importlib.util.spec_from_file_location("mutation_score", _SCRIPT)
mutation_score = importlib.util.module_from_spec(_spec)
assert _spec.loader is not None
_spec.loader.exec_module(mutation_score)

ScoreError = mutation_score.ScoreError
score_from_stats = mutation_score.score_from_stats


def _stats(**over) -> dict:
    base = {
        "killed": 80,
        "survived": 20,
        "total": 100,
        "no_tests": 0,
        "skipped": 0,
        "suspicious": 0,
        "timeout": 0,
        "check_was_interrupted_by_user": 0,
        "segfault": 0,
    }
    base.update(over)
    return base


@pytest.mark.unit
class TestScore:
    def test_exactly_at_floor_passes(self):
        score, killed, tested = score_from_stats(_stats(killed=80, survived=20), 80)
        assert (score, killed, tested) == (80.0, 80, 100)

    def test_below_floor_is_computable_so_the_caller_can_fail(self):
        score, _, _ = score_from_stats(_stats(killed=60, survived=40), 80)
        assert score == 60.0

    def test_timeout_counts_as_killed(self):
        # A mutant that hangs has changed behaviour; the tests caught it.
        score, neutralised, _ = score_from_stats(_stats(killed=70, timeout=10, survived=20), 80)
        assert (score, neutralised) == (80.0, 80)

    def test_skipped_mutants_are_excluded_from_the_denominator(self):
        score, killed, tested = score_from_stats(
            _stats(killed=80, survived=20, skipped=50, total=150), 80
        )
        assert tested == 100
        assert (score, killed) == (80.0, 80)

    def test_every_mutant_skipped_raises_instead_of_scoring_100(self):
        # The bug this closes: ci.yml scored tested <= 0 as 100.0.
        with pytest.raises(ScoreError, match="no mutants were tested"):
            score_from_stats(_stats(killed=0, survived=0, skipped=100, total=100), 80)

    def test_no_mutants_at_all_raises(self):
        with pytest.raises(ScoreError, match="no mutants were tested"):
            score_from_stats(_stats(total=0, killed=0, survived=0, skipped=0), 80)

    def test_missing_count_is_rejected(self):
        stats = _stats()
        del stats["killed"]
        with pytest.raises(ScoreError, match="missing killed"):
            score_from_stats(stats, 80)

    def test_non_integer_count_is_rejected(self):
        with pytest.raises(ScoreError, match="non-integer"):
            score_from_stats(_stats(killed="eighty"), 80)


@pytest.mark.unit
class TestMain:
    def _write(self, tmp_path: Path, payload) -> Path:
        path = tmp_path / "stats.json"
        path.write_text(
            payload if isinstance(payload, str) else json.dumps(payload), encoding="utf-8"
        )
        return path

    def test_missing_file_fails_loudly(self, tmp_path, capsys):
        rc = mutation_score.main(["--stats", str(tmp_path / "nope.json")])
        assert rc == 1
        assert "cannot read" in capsys.readouterr().out

    def test_corrupt_json_fails_loudly(self, tmp_path, capsys):
        path = self._write(tmp_path, "{not json")
        assert mutation_score.main(["--stats", str(path)]) == 1
        assert "not valid JSON" in capsys.readouterr().out

    def test_passing_score_exits_zero(self, tmp_path, capsys):
        path = self._write(tmp_path, _stats(killed=80, survived=20))
        assert mutation_score.main(["--stats", str(path), "--min-score", "80"]) == 0
        assert "80.0%" in capsys.readouterr().out

    def test_failing_score_exits_one(self, tmp_path, capsys):
        path = self._write(tmp_path, _stats(killed=60, survived=40))
        assert mutation_score.main(["--stats", str(path), "--min-score", "80"]) == 1
        assert "below the" in capsys.readouterr().out

    def test_all_skipped_exits_one_not_zero(self, tmp_path, capsys):
        path = self._write(tmp_path, _stats(killed=0, survived=0, skipped=100, total=100))
        assert mutation_score.main(["--stats", str(path), "--min-score", "80"]) == 1
        assert "no mutants were tested" in capsys.readouterr().out

    def test_min_score_reads_the_env_var(self, tmp_path, monkeypatch):
        # ci.yml passes the threshold as an env var, so the default must honour it.
        path = self._write(tmp_path, _stats(killed=70, survived=30))
        monkeypatch.setenv("MUTATION_SCORE_MIN", "60")
        assert mutation_score.main(["--stats", str(path)]) == 0
        monkeypatch.setenv("MUTATION_SCORE_MIN", "90")
        assert mutation_score.main(["--stats", str(path)]) == 1

    def test_arg_overrides_the_env_var(self, tmp_path, monkeypatch):
        path = self._write(tmp_path, _stats(killed=70, survived=30))
        monkeypatch.setenv("MUTATION_SCORE_MIN", "90")
        assert mutation_score.main(["--stats", str(path), "--min-score", "60"]) == 0


@pytest.mark.unit
def test_script_is_executable_as_a_module():
    # Guards the __main__ path, which is how CI invokes it.
    assert _SCRIPT.exists()
    source = _SCRIPT.read_text(encoding="utf-8")
    assert 'if __name__ == "__main__":' in source
