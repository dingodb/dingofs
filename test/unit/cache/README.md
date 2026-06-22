# How to run

## Step 1: build

```bash
cmake -S . -B build \
  -DBUILD_UNIT_TESTS=ON \
  -DCMAKE_BUILD_TYPE=RelWithDebInfo \
  -DCMAKE_EXPORT_COMPILE_COMMANDS=ON \
&& cmake --build build --target test_cache -j 32
```

If you want test coverage:

```bash
cmake -S . -B build \
  -DBUILD_UNIT_TESTS=ON \
  -DCMAKE_BUILD_TYPE=Debug \
  -DCMAKE_EXPORT_COMPILE_COMMANDS=ON \
  -DCMAKE_C_FLAGS="--coverage -O0 -g -fprofile-update=atomic" \
  -DCMAKE_CXX_FLAGS="--coverage -O0 -g -fprofile-update=atomic" \
  -DCMAKE_EXE_LINKER_FLAGS="--coverage" \
  -DCMAKE_SHARED_LINKER_FLAGS="--coverage" \
&& cmake --build build --target test_cache -j 32
```

## Step 2: run

```bash
./build/bin/test/test_cache --gtest_filter='*'
```

or

```bash
./build/bin/test/test_cache --coverage
```

With `--coverage`, after the tests it: runs `gcovr` over `src/cache/`
(`build/coverage/`), prints a per-file line-coverage table, and serves
the HTML report at `http://127.0.0.1:<random-port>/` (Ctrl-C to stop).

Notes:
- Needs `gcovr` and `python3` on `PATH`, plus a `gcov` matching the compiler
  (this repo uses `gcc-toolset-13`).
- Each `--coverage` run wipes stale `.gcda` first, so the report reflects only
  that run.
