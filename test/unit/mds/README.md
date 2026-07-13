# MDS Unit Tests with Coverage

This directory contains unit tests for the MDS (Metadata Service) component.

## Build Prerequisites

To build with coverage instrumentation, configure CMake with the following flags:

```bash
cmake -S . -B build \
  -DBUILD_UNIT_TESTS=ON \
  -DCMAKE_BUILD_TYPE=Debug \
  -DCMAKE_EXPORT_COMPILE_COMMANDS=ON \
  -DENABLE_COVERAGE=ON
```

## Building and Running

Build the MDS test target:

```bash
cmake --build build --target test_mds -j 12
./build/bin/test/test_mds --coverage
```

## Coverage Report

Coverage data is generated in `build/coverage/test_mds/`.

The binary prints a per-file line-coverage table. The HTML report is written to `build/coverage/test_mds/index.html`; open it directly in a browser. No HTTP server is started.

### Requirements

- `gcovr` — coverage report generator
- `python3` — required by gcovr
- `gcov` version matching your compiler

### Notes

Each test run removes stale `.gcda` files before execution to ensure clean coverage data.
