# DingoFS End-to-End Tests

Black-box tests that run against a deployed `dingo-client` mount.

Unlike `test/unit/` (per-module C++ unit tests) and `test/integration/`
(C++ multi-component in-process integration), this suite drives the
**full stack**: FUSE kernel module → `dingo-client` → MDS / Local /
Memory metasystem → block store (S3 / file). The test author can
exercise DingoFS-specific behavior — chunk boundaries, mode-specific
paths, regression fixtures keyed to commit hashes — without having to
link against C++ internals.

## Scope

| Layer | Tested by | Verifies |
|---|---|---|
| C++ unit (`test/unit/`) | gtest, in-process | one class / one module behavior |
| C++ integration (`test/integration/`) | gtest, in-process | several C++ modules wired together |
| **e2e (here)** | **pytest, via FUSE mount** | **whole stack including FUSE / userspace client / MDS / S3** |

## Layout

```
test/e2e/
├── conftest.py            # --mount-point arg + test_dir fixture
├── pyproject.toml         # uv project
├── pytest.ini             # markers (smoke / standard) + addopts
├── README.md              # this file
├── posix/                 # vendor-neutral POSIX baseline (transitional)
│   └── test_*.py
├── regression/            # Bug-fix regression keyed to specific commits
│   └── test_regression_*.py
└── specific/              # DingoFS-specific layout / data path
    └── test_*.py
```

### `posix/` — Transitional POSIX baseline

These tests cover basic POSIX FS semantics that any compliant filesystem
should satisfy (read / write / mkdir / chmod / link / rename / xattr,
…). They are a **transitional** stub maintained until pjdfstest
(~8800 standards-grade POSIX cases) is integrated, at which point this
whole directory will be deleted.

Maintenance policy:
- Smoke-level coverage only — exhaustive POSIX compliance is pjdfstest's
  job, not this directory's.
- When adding a new test, first check whether pjdfstest already covers
  it; if so, do not duplicate here.

### `regression/` — Bug-fix regression

Tests bound to a specific bug-fix commit. Each file's header docstring
must declare:
- **Bug**: observable symptom and where it manifests
- **Fix**: PR # / commit hash and a one-line summary of the fix
- **Verification**: which test fails pre-fix (with symptom), what passes
  post-fix, and which modes were covered (MDS / Local / Memory)

File naming: `test_regression_<keyword>.py`. The prefix is intentional —
it signals "this exists to keep a specific fix from regressing", and the
file header tells the future maintainer exactly which fix.

### `specific/` — DingoFS-specific behavior

Tests whose assertions depend on knowing DingoFS internal data layout
(64 MiB chunk size, 4 MiB block size, slice mechanism, id=0 sparse
sentinel, etc.). They are not portable to ext4 / JuiceFS because the
constants and the data-plane choices differ.

File naming: `test_<topic>.py` (no `test_regression_` prefix — these are
behavior probes, not bug regressions).

Each file's header docstring should note which DingoFS internal detail
it exercises (e.g. "validates writes at 64 MiB chunk boundary",
"verifies sparse hole representation crossing chunks").

## Running

```bash
cd test/e2e

# Install deps (creates .venv via uv)
uv sync

# Run against an MDS-mode mount
uv run pytest --mount-point=/home/me/mounts/claude-mount/bench-vs-fs

# Smoke only
uv run pytest --mount-point=<MP> -m smoke

# Single file
uv run pytest regression/test_regression_fallocate_modes.py --mount-point=<MP>
```

> Run the appropriate `bash scripts/deploy/{mds,local,memory}/deploy_all.sh`
> before invoking pytest to ensure a fresh mountpoint.

## Authoring a new regression test

```python
# regression/test_regression_<keyword>.py
"""Regression test for <bug summary>.

Bug:
  <observable symptom; where it manifests; affected modes>

Fix: PR #<num> — <one-line summary>
  (optionally list sub-commits with [tag][scope] headlines)

Verification:
  pre <fix>:  <which test should FAIL with what symptom>
  post <fix>: <expected behavior>
  works on: MDS / Local / Memory (mark per-test if mode-specific)
"""
import os, pytest


@pytest.mark.smoke
def test_the_bug(test_dir):
    path = os.path.join(test_dir, "case")
    ...
```
