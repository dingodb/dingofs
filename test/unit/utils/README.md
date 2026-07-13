# Utils Unit Tests with Coverage

This directory contains unit tests for utility functions and helpers.

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

Build the utils test target:

```bash
cmake --build build --target test_utils -j 12
./build/bin/test/test_utils --coverage
```

## Coverage Report

Coverage data is generated in `build/coverage/test_utils/`.

The binary prints a per-file line-coverage table. The HTML report is written to `build/coverage/test_utils/index.html`; open it directly in a browser. No HTTP server is started.

### Requirements

- `gcovr` — coverage report generator
- `python3` — required by gcovr
- `gcov` version matching your compiler

### Notes

Each test run removes stale `.gcda` files before execution to ensure clean coverage data.
