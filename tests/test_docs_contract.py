"""The docs have to keep up with the code, or they are decoration.

Two claims are cheap to make and easy to break: that a `make X` target exists, and
that every knob the code reads is written down somewhere. Both had already drifted,
so they are checked here instead of trusted. The scan is deliberately mechanical:
it reads what the docs say and what the code does, and it fails on the difference.

Each scan carries a floor on how much it found. A doc that stops naming make targets,
or code that stops reading the environment, must not turn these into green tests that
check nothing.
"""

import os
import re

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# The two spellings of "read an environment variable" this repo uses.
_ENV_READS = (
    re.compile(r"""os\.environ\.get\(\s*["']([A-Z][A-Z0-9_]*)["']"""),
    re.compile(r"""os\.environ\.setdefault\(\s*["']([A-Z][A-Z0-9_]*)["']"""),
    re.compile(r"""os\.environ\[\s*["']([A-Z][A-Z0-9_]*)["']"""),
    re.compile(r"""os\.getenv\(\s*["']([A-Z][A-Z0-9_]*)["']"""),
)

# Read by the code, deliberately absent from .env.example, with the reason.
_NOT_A_KNOB = {
    "TEMP": "the OS scratch directory; src/common/settings.py falls back to /tmp",
}


def _read(path):
    with open(path, encoding="utf-8", errors="replace") as fh:
        return fh.read()


def _docs():
    """Every markdown file a reader would open, not the ones under .venv or .git."""
    paths = [
        os.path.join(ROOT, name)
        for name in os.listdir(ROOT)
        if name.endswith(".md") and os.path.isfile(os.path.join(ROOT, name))
    ]
    docs_dir = os.path.join(ROOT, "docs")
    if os.path.isdir(docs_dir):
        paths += [
            os.path.join(docs_dir, name)
            for name in sorted(os.listdir(docs_dir))
            if name.endswith(".md")
        ]
    return sorted(paths)


def _make_targets():
    """Targets the Makefile really defines, by rule and by .PHONY."""
    text = _read(os.path.join(ROOT, "Makefile"))
    found = set(re.findall(r"^([A-Za-z][A-Za-z0-9_.\-]*)\s*:", text, re.M))
    for line in text.splitlines():
        if line.startswith(".PHONY:"):
            found |= set(line.split()[1:])
    return found


def _env_knobs():
    """Every environment variable src/ and scripts/ read, mapped to where it is read."""
    found = {}
    for folder in ("src", "scripts"):
        base = os.path.join(ROOT, folder)
        for dirpath, _dirs, files in os.walk(base):
            for name in sorted(files):
                if not name.endswith(".py"):
                    continue
                path = os.path.join(dirpath, name)
                rel = os.path.relpath(path, ROOT).replace(os.sep, "/")
                for lineno, line in enumerate(_read(path).splitlines(), 1):
                    if line.lstrip().startswith("#"):
                        continue
                    for pattern in _ENV_READS:
                        for match in pattern.finditer(line):
                            found.setdefault(match.group(1), f"{rel}:{lineno}")
    return found


def _env_example_keys():
    """Keys declared in .env.example, commented-out ones included: those are still written down."""
    keys = set()
    for line in _read(os.path.join(ROOT, ".env.example")).splitlines():
        match = re.match(r"\s*#?\s*([A-Z][A-Z0-9_]*)\s*=", line)
        if match:
            keys.add(match.group(1))
    return keys


def test_the_make_targets_the_docs_name_exist():
    """A `make X` in a doc is a promise that X is a target.

    Renaming the mix gate to `accuracy` left BENCHMARKS.md telling the reader to run
    `make mix`, a target that has never existed.
    """
    defined = _make_targets()
    named = {}
    for path in _docs():
        for lineno, line in enumerate(_read(path).splitlines(), 1):
            for match in re.finditer(r"`make\s+([A-Za-z][A-Za-z0-9_.\-]*)", line):
                named.setdefault(match.group(1), f"{os.path.basename(path)}:{lineno}")
    assert len(named) >= 4, f"only found {len(named)} make targets in the docs, so this is asleep"
    missing = {name: at for name, at in named.items() if name not in defined}
    assert not missing, (
        f"the docs invoke make targets the Makefile does not define: {missing}. "
        f"Defined: {sorted(defined)}"
    )


def test_every_make_target_is_written_down():
    """The README's table is a map of the Makefile, so it may not fall behind it."""
    defined = _make_targets()
    assert len(defined) >= 15, f"only found {len(defined)} targets, so this is asleep"
    readme = _read(os.path.join(ROOT, "README.md"))
    missing = [name for name in sorted(defined) if f"`make {name}`" not in readme]
    assert not missing, (
        f"the Makefile defines {missing} and README.md never names them: a target "
        "nobody can find from the front door is a target nobody runs"
    )


def test_every_env_knob_the_code_reads_is_written_down():
    """An undocumented knob is a knob nobody knows to turn."""
    read = _env_knobs()
    assert len(read) >= 15, f"only found {len(read)} env reads, so this is asleep"
    keys = _env_example_keys()
    missing = {
        name: at for name, at in read.items() if name not in keys and name not in _NOT_A_KNOB
    }
    assert not missing, (
        f"the code reads these and .env.example does not declare them: {missing}. "
        "Add the key with a comment saying what it does, or record why it does not belong "
        f"there in _NOT_A_KNOB. Known exclusions: {_NOT_A_KNOB}"
    )


def test_the_known_exclusions_are_still_being_read():
    """An exemption for a variable the code no longer reads is a stale excuse."""
    read = _env_knobs()
    stale = {name: reason for name, reason in _NOT_A_KNOB.items() if name not in read}
    assert not stale, f"exempted but no longer read: {stale}"
    for name, reason in _NOT_A_KNOB.items():
        assert reason.strip(), f"{name} is exempted without saying why"


@pytest.mark.parametrize("folder", ["src", "scripts"])
def test_the_env_scan_is_reading_the_code(folder):
    """Both folders have to parse, or a moved module stops being checked."""
    base = os.path.join(ROOT, folder)
    assert os.path.isdir(base), f"{folder}/ is gone; this scan no longer knows where to look"
    assert any(name.endswith(".py") for name in os.listdir(base))


def _mermaid_inits():
    """Every %%{init}%% line in the shipped markdown, with where it lives."""
    found = []
    for path in _docs():
        for lineno, line in enumerate(_read(path).splitlines(), 1):
            if "%%{init:" in line:
                found.append((os.path.relpath(path, ROOT), lineno, line))
    return found


def test_the_diagrams_do_not_lock_a_theme():
    """A locked theme breaks dark mode, and it did: lanes rendered near-black.

    GitHub adjusts an unlocked diagram for the reader's mode, but a diagram that
    names its own theme keeps those colours everywhere. The init may pin the
    renderer (look, layout) and the blocks may colour their own nodes, because an
    explicit fill travels with its explicit text colour. What it may not do is
    name a theme, carry themeVariables, or fill a lane: those are page-dependent
    and belong to the reader's mode, not the diagram.
    """
    inits = _mermaid_inits()
    assert len(inits) >= 3, f"only {len(inits)} mermaid init lines found, so this check is asleep"
    for rel, lineno, line in inits:
        assert '"theme"' not in line, (
            f"{rel}:{lineno} locks a mermaid theme, which disables the reader's "
            "dark-mode adjustment"
        )
        assert "themeVariables" not in line, (
            f"{rel}:{lineno} carries themeVariables, which are fixed light colours on a dark page"
        )
    lanes = []
    for path in _docs():
        for lineno, line in enumerate(_read(path).splitlines(), 1):
            if re.match(r"\s*style\s+(SL|BL|POST)\b", line):
                lanes.append(f"{os.path.relpath(path, ROOT)}:{lineno}")
    assert not lanes, (
        "lane fills are page-dependent !important paint; they stay light while the "
        f"page goes dark: {lanes}"
    )
