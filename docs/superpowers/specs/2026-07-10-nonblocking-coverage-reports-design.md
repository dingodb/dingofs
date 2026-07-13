# Nonblocking Coverage Reports Design

## Goal

Make `test_common --coverage`, `test_mds --coverage`, and
`test_utils --coverage` print coverage statistics and exit normally.

## Behavior

The shared coverage helper continues to clear counters, flush GCC data, run
gcovr, print its per-file line-coverage table, and write HTML, CSV, and text
reports under `build/coverage/<target>/`.

After reporting, it prints the `index.html` path and returns the test result.
It does not bind a port, start `python3 -m http.server`, or replace the test
process. This applies uniformly to the three targets because they share the
helper.

## Documentation and Validation

Update the three target READMEs to say that the HTML report is written locally
and can be opened directly; it is not served automatically. Validate the
coverage helper test and an instrumented coverage invocation exits without a
long-running HTTP server while producing `index.html` and `summary.csv`.
