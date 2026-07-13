# Targeted Unit Test Coverage Design

## Goal

Give `test_mds`, `test_common`, and `test_utils` the same opt-in,
per-binary coverage workflow that already exists for `test_cache`. Each
instrumented test binary will generate an isolated report for the source
subsystem it exercises.

## Scope

Add a reusable test-only coverage helper under `test/unit/`. The helper owns
the `--coverage` gflag and the mechanics shared by the three test binaries:

1. Locate the build directory from `/proc/self/exe`.
2. Remove stale `.gcda` files before the test run.
3. Flush GCC's in-memory counters after the test run.
4. Run `gcovr`, print a per-file line-coverage table, and write reports.
5. Serve the HTML report on a loopback ephemeral port.

The helper will receive each target's report title and source filter:

| Binary | Source filter | Report directory |
| --- | --- | --- |
| `test_mds` | `src/mds/` | `build/coverage/test_mds/` |
| `test_common` | `src/common/` | `build/coverage/test_common/` |
| `test_utils` | `src/utils/` | `build/coverage/test_utils/` |

Each report directory contains `index.html`, `summary.txt`, and
`summary.csv`. Clearing counters before execution ensures the report reflects
only that invocation.

`test_cache` and its coverage implementation are not changed.

## Behavior

Without `--coverage`, all three binaries preserve their current behavior.
With `--coverage`, the selected binary runs its usual tests and then produces
its own report. A binary built without GCC coverage instrumentation fails with
a clear message rather than producing an empty report. Failures from tests,
`gcovr`, directory creation, or report serving preserve a nonzero result.

The coverage helper uses weak linkage for `__gcov_dump`, so normal,
non-instrumented builds link unchanged. Developers configure instrumentation
using the existing cache-compatible CMake flags:

```text
--coverage -O0 -g -fprofile-update=atomic
```

for C and C++, with `--coverage` passed to executable and shared-library
linkers.

## Documentation and Validation

Add coverage instructions to the unit-test documentation for MDS, common, and
utils. Instructions include the instrumented CMake configuration, target build
command, `--coverage` invocation, output location, and the `gcovr`/matching
`gcov` prerequisite.

Validate the shared helper through the existing test build and by confirming
the non-instrumented `--coverage` failure path. No CI integration, coverage
threshold, or aggregate report is introduced.
