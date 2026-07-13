# ENABLE_COVERAGE CMake Option Design

## Goal

Replace manual coverage compiler and linker flags with a single
`-DENABLE_COVERAGE=ON` CMake option.

## Behavior

`ENABLE_COVERAGE` defaults to `OFF`. Default builds do not receive coverage
instrumentation.

When enabled, CMake requires both C and C++ compilers to be GNU. Otherwise,
configuration fails with a clear error. For a GNU build, the project adds:

```text
--coverage -O0 -g -fprofile-update=atomic
```

to compilation, and `--coverage` to linking. These project-wide options
instrument the production libraries linked by unit tests as well as the test
binaries.

The existing `--coverage` test-binary flag and report behavior are unchanged.
`test_cache` remains unchanged.

## Documentation and Validation

The MDS, common, utils, and cache unit-test READMEs use
`-DENABLE_COVERAGE=ON` instead of manual `CMAKE_*_FLAGS`. Validate that the
default configuration has no coverage flags, an enabled GNU configuration
does, and an enabled non-GNU configuration is rejected.
