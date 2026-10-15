"""Two test modules imported `tests.<subpkg>`, which resolves nowhere.

There is no `tests/__init__.py` in this repo, so `tests` is only importable as a
namespace package assembled from sys.path. That loses to any *regular* `tests`
package earlier on the path, and this machine has one: an unrelated
distribution installed `site-packages/tests/__init__.py`. The result is a
ModuleNotFoundError at collection time for two files, on the developer's machine
only -- CI on Linux passes, so nothing catches it until someone runs the suite
locally.

The repo's own convention already works: pytest puts each test file's directory
on sys.path, so a helper lives next to the tests that use it and is imported by
its bare module name (`from test_reconcile import _run_with_collects`). This
guards that, because the failure is invisible from CI.
"""

from __future__ import annotations

import ast
import os
from pathlib import Path

TESTS_DIR = Path(__file__).resolve().parent


def _package_of(node: ast.stmt) -> str | None:
    if isinstance(node, ast.ImportFrom):
        return node.module or ""
    if isinstance(node, ast.Import) and node.names:
        return node.names[0].name
    return None


def _imports_of_tests_package() -> list[str]:
    found = []
    for path in sorted(TESTS_DIR.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            module = _package_of(node)
            if module and module.split(".")[0] == "tests":
                rel = os.path.relpath(path, TESTS_DIR)
                found.append(f"{rel}:{node.lineno} imports {module}")
    return found


def test_no_test_module_imports_the_tests_package():
    offenders = _imports_of_tests_package()
    assert not offenders, (
        "these import `tests.<subpkg>`, which is not importable here:\n  "
        + "\n  ".join(offenders)
        + "\nimport the helper by its bare module name instead, from the "
        "directory pytest already put on sys.path"
    )
