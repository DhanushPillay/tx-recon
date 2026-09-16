"""Smoke: every src module imports (honest coverage, no hidden files)."""

import pkgutil

import src


def test_all_modules_importable():
    mods = [m.name for m in pkgutil.walk_packages(src.__path__, "src.")]
    assert len(mods) > 10
    for name in mods:
        __import__(name)
