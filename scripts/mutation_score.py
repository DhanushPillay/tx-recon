"""Assert a mutation score from mutmut's CI/CD stats file.

`mutmut run` writes mutants/mutmut-stats.json, which is a resume cache and has
no counts in it. `mutmut export-cicd-stats` reads the per-mutant exit codes and
writes mutants/mutmut-cicd-stats.json, which does: killed, survived, total,
skipped, timeout and friends. This script gates on the second one.

Kept as a module rather than an inline `python -c` so the arithmetic is unit
testable without running mutmut, which needs WSL and takes hours.

Usage:
    python scripts/mutation_score.py --min-score 80
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

DEFAULT_STATS = Path("mutants/mutmut-cicd-stats.json")

# Statuses that mean the mutant was neutralised. A timeout counts: the mutant
# hangs, which is a behaviour change the tests caught.
KILLING_KEYS = ("killed", "timeout")


class ScoreError(RuntimeError):
    """The score cannot be computed, so it must not be treated as passing."""


def score_from_stats(stats: dict, min_score: float) -> tuple[float, int, int]:
    """Return (score, killed, tested) or raise ScoreError.

    tested counts mutants that actually ran: mutmut reports skipped mutants
    inside `total`, and a skipped mutant says nothing about test strength.
    """
    missing = [k for k in ("total", "skipped", "killed", "survived") if k not in stats]
    if missing:
        raise ScoreError(
            f"stats file is missing {', '.join(missing)}; is it from export-cicd-stats?"
        )

    try:
        total = int(stats["total"])
        skipped = int(stats["skipped"])
        for k in KILLING_KEYS + ("survived",):
            int(stats[k])
    except (TypeError, ValueError) as exc:
        raise ScoreError(f"stats file has non-integer counts: {exc}") from exc

    tested = total - skipped
    if tested <= 0:
        # Every mutant was skipped, or there were none at all. Either way no
        # test killed anything, so this is a zero, never a pass.
        raise ScoreError(
            f"no mutants were tested (total={total}, skipped={skipped}); "
            "check paths_to_mutate and tests_dir in pyproject.toml"
        )

    neutralised = sum(int(stats.get(k, 0)) for k in KILLING_KEYS)
    return neutralised / tested * 100, neutralised, tested


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--stats", type=Path, default=DEFAULT_STATS, help="path to mutmut-cicd-stats.json"
    )
    parser.add_argument(
        "--min-score",
        type=float,
        default=float(os.environ.get("MUTATION_SCORE_MIN", "80")),
        help="fail below this percentage (default: $MUTATION_SCORE_MIN, else 80)",
    )
    args = parser.parse_args(argv)

    try:
        raw = args.stats.read_text(encoding="utf-8")
    except OSError as exc:
        print(f"FAIL: cannot read {args.stats}: {exc}")
        print("Did `mutmut run` finish, and was `mutmut export-cicd-stats` run after it?")
        return 1

    try:
        stats = json.loads(raw)
    except json.JSONDecodeError as exc:
        print(f"FAIL: {args.stats} is not valid JSON: {exc}")
        return 1

    try:
        score, neutralised, tested = score_from_stats(stats, args.min_score)
    except ScoreError as exc:
        print(f"FAIL: {exc}")
        return 1

    survived = int(stats.get("survived", 0))
    print(f"mutation score {score:.1f}% (min {args.min_score:.1f}%)")
    print(
        f"  {neutralised}/{tested} mutants killed, {survived} survived, {int(stats['skipped'])} skipped"
    )
    if survived:
        print("  surviving mutants are test-strength gaps, not infrastructure failures")

    if score < args.min_score:
        print(f"FAIL: {score:.1f}% is below the {args.min_score:.1f}% floor")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
