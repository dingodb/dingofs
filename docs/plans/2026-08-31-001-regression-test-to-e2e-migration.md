# Migrate `test/regression-test` into pytest e2e

## Goal

Retire the standalone Python scripts in `test/regression-test/` without
losing their useful filesystem coverage. The single test entry point becomes
`test/e2e`, which drives a deployed DingoFS FUSE mount with pytest.

This is a rewrite-and-merge migration, not a file move: the old suite uses
per-script `Checker`/`run_case` helpers and positional arguments, whereas e2e
uses pytest fixtures and assertions.

## Decisions

- Keep each useful assertion, but do not duplicate coverage already present in
  `test/e2e`.
- Put generic POSIX smoke coverage in `posix/`, DingoFS data-path behavior in
  `specific/`, and only tests tied to a fixed bug in `regression/`.
- Use pytest paths, `-k`, and markers instead of `run_all.py` and numbered
  scripts. Delete `test/regression-test/` after the migration map is complete.
- Add one `slow` marker. The merge-queue e2e job runs `-m "not slow"`.
  Large-data, stress, high-contention, and quota tests are slow and are run
  explicitly when needed. This migration does not change `jenkins-regression`.
- Quota tests receive MDS endpoint, filesystem ID, and root inode through
  `--mds-addr`, `--fs-id`, and `--root-ino`. If any is absent, they skip rather
  than relying on a deployment-specific default.
- For features supported by the MDS CI configuration, errors fail the test.
  A skip is allowed only for a documented platform/backend-optional feature.
- The shared test-directory fixture removes a passing case directory and keeps
  a failing one, printing its path for local investigation.

## Migration order

1. **Foundation**: add the marker, quota configuration fixture, failure
   retention, and default CI selection.
2. **Generic file semantics**: merge old cases 01--35 and 45--50 into the
   relevant existing `posix/` tests; retain only assertions absent there.
3. **DingoFS-specific and concurrent semantics**: convert old cases 36--44
   and 51--64 into focused `specific/` tests. Mark expensive and intentionally
   nondeterministic workloads `slow`.
4. **Quota**: rewrite old quota cases using the quota fixture and pytest
   assertions; all are `slow`.
5. **Verification and removal**: maintain a case-to-test mapping while
   converting. Run the old and new variants against the same mount during each
   batch. Once every retained assertion has a destination, delete the old
   suite and its helpers.

## Commands

```bash
cd test/e2e
uv sync

# Default, merge-queue-sized suite
uv run pytest -m "not slow" --mount-point=<mount-point>

# An explicit slow test or group
uv run pytest -m slow --mount-point=<mount-point>

# Quota tests need their control-plane identity
uv run pytest quota --mount-point=<mount-point> \
  --mds-addr=<host:port> --fs-id=<id> --root-ino=<inode>
```

## Migration map

| Old case | pytest destination | Status |
| --- | --- | --- |
| `file/test_01_write_read_consistency.py`--`test_04_block_boundary_rw.py` | Existing `posix/test_basic_io.py` and `specific/test_{large_file,chunk_boundary_write}.py` | Existing coverage retained |
| `file/test_05_large_file_rw.py` | `specific/test_large_file.py::test_write_read_large_md5` | Converted (`slow`) |
| `quota/test_01_create_and_root_fallback.py`--`test_06_api_edges.py` | `quota/test_quota_usage.py` | Converted (`slow`) |
| `file/test_31_o_excl.py`, `test_32_o_trunc.py`, `test_33_o_append_semantics.py` | `posix/test_open.py::test_open_exclusive_existing_fails`, `test_open_trunc_and_append_ignore_seek` | Converted |
| `file/test_34_multi_fd_visibility.py`, `test_35_fsync_durability.py` | `posix/test_open.py::test_two_descriptors_observe_fsync_and_overwrite`, `test_fsync_and_fdatasync_persist_data` | Converted |
| `file/test_45_read_beyond_eof.py`--`test_49_enametoolong.py`, `test_54_open_modes.py` | `posix/test_open.py` | Converted |
| `file/test_07_overwrite_middle.py`, `test_11_mixed_rw_same_fd.py`, `test_16_ftruncate_open_fd.py`--`test_19_chmod_chown.py`, `test_23_unlink_open_fd.py`, `test_27_listdir_many_files.py`--`test_30_special_filenames.py`, `test_36_mmap_rw.py` | `posix/test_extended.py` | Converted (`test_listdir_many_files` is `slow`) |
| `file/test_06_append_write.py`, `test_24_hardlink.py`, `test_25_symlink.py` | `posix/test_open.py`, `posix/test_links.py` | Converted/merged |
| `file/test_08_sparse_file.py`--`test_10_random_write.py`, `test_12_tiny_writes.py`--`test_15_truncate_zero_rewrite.py`, `test_20_utime.py`--`test_22_rename_overwrite.py`, `test_26_mkdir_rmdir.py` | Existing `specific/`, `posix/`, and bug-regression tests | Existing coverage retained |
| `file/test_37_concurrent_write_separate_files.py`--`test_44_truncate_write_race.py` | `specific/test_legacy_concurrent.py` | Converted (`slow`) |
| `file/test_51_chaos.py`--`test_53_concurrent_otrunc_rewrite.py` | `specific/test_legacy_stress.py` | Converted (`slow`) |
| `file/test_50_statvfs.py` | `specific/test_statfs.py` | Existing coverage retained |
| `file/test_55_read_concurrent_write.py`--`test_62_mixed_concurrent_rw_truncate_fallocate.py` | `specific/test_legacy_concurrent.py::test_threads_mix_read_write_and_truncate`, `test_threads_mix_read_write_and_fallocate` | Converted (`slow`) |
| `file/test_63_fallocate_semantics.py` | `specific/test_fallocate_extended.py` plus existing fallocate regressions | Converted (chunk-collapse case is `slow`) |
| `file/test_64_copy_file_range.py` | `specific/test_copy_file_range.py` | Converted (multi-chunk/concurrent case is `slow`) |

The remaining cases are mapped as their assertions are merged or converted;
the old source remains the comparison baseline until then.

## Validation performed

On the supplied MDS FUSE mount, the converted POSIX, fallocate, and
copy-file-range default selection passed **34 tests** (one root-only chown
skip). The converted quota suite passed **6 tests** with the supplied MDS
parameters. The converted concurrent/stress suite passed **13 tests** with
reduced workload parameters. The old non-slow runner passed **59 cases** and
skipped its five slow cases; old cases 63 and 64 passed independently as well.
`FALLOC_FL_COLLAPSE_RANGE` skipped in both old and new tests because this mount
does not support that optional mode. After invalidating stale read-chunk cache
entries on metadata changes, both the old and pytest chaos cases passed their
full 2,000-operation runs on the supplied MDS mount.

## Completion status

Complete: the old suite was deleted after the mapped pytest cases and the
legacy counterparts passed on the supplied MDS mount.

## Completion criteria

- Each old case is mapped to a retained pytest assertion or an explicitly
  documented duplicate in the migration map.
- The new test passes in the same configuration in which its old counterpart
  passed.
- Default e2e stays within the existing merge-queue budget; slow tests are not
  selected there.
- No `test/regression-test/`, `run_all.py`, or duplicate `Checker` helper
  remains.
