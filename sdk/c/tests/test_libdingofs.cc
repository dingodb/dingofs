/*
 * Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * test_libdingofs.cc — unit tests for the libdingofs C API
 *
 * Tests are divided into two groups:
 *
 *   Group 1 – No cluster required
 *     These tests exercise parameter validation, handle lifecycle, and
 *     conf_set / conf_get round-trips.  They run on any machine.
 *
 *   Group 2 – Cluster required
 *     Full I/O tests.  Skipped when DINGOFS_MDS_ADDRS is not set.
 */

#include <cassert>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <sys/stat.h>

#include "libdingofs.h"

/* ============================================================================
 * Minimal test runner
 * ============================================================================ */

static int g_pass = 0;
static int g_fail = 0;
static int g_skip = 0;

#define PASS(name)  do { printf("[ PASS ] %s\n", (name)); ++g_pass; } while (0)
#define FAIL(name, msg) \
    do { printf("[ FAIL ] %s — %s\n", (name), (msg)); ++g_fail; } while (0)
#define SKIP(name, reason) \
    do { printf("[ SKIP ] %s — %s\n", (name), (reason)); ++g_skip; } while (0)

/* Assert helper: evaluates expr, fails the test and returns on mismatch. */
#define EXPECT_EQ(name, actual, expected)                               \
    do {                                                                \
        int _a = (int)(actual);                                         \
        int _e = (int)(expected);                                       \
        if (_a != _e) {                                                 \
            printf("[ FAIL ] %s — expected %d, got %d\n",              \
                   (name), _e, _a);                                     \
            ++g_fail;                                                   \
            return;                                                     \
        }                                                               \
    } while (0)

#define EXPECT_NE(name, actual, expected)                               \
    do {                                                                \
        if ((actual) == (expected)) {                                   \
            printf("[ FAIL ] %s — unexpected value %d\n",              \
                   (name), (int)(expected));                            \
            ++g_fail;                                                   \
            return;                                                     \
        }                                                               \
    } while (0)

#define EXPECT_LT(name, actual, val)                                    \
    do {                                                                \
        if ((actual) >= (val)) {                                        \
            printf("[ FAIL ] %s — expected < %d, got %d\n",            \
                   (name), (int)(val), (int)(actual));                  \
            ++g_fail;                                                   \
            return;                                                     \
        }                                                               \
    } while (0)

/* ============================================================================
 * Group 1: no cluster needed
 * ============================================================================ */

/* dingofs_new / dingofs_delete */
static void test_new_delete() {
    const char* name = "new_delete";
    uintptr_t h = dingofs_new();
    EXPECT_NE(name, h, 0u);
    dingofs_delete(h);
    PASS(name);
}

/* Calling dingofs_delete(0) must not crash. */
static void test_delete_null() {
    dingofs_delete(0);
    PASS("delete_null");
}

/* All functions with a handle parameter must return -EINVAL for h=0. */
static void test_null_handle() {
    const char* name = "null_handle_returns_EINVAL";

    EXPECT_EQ(name, dingofs_conf_set(0, "k", "v"),            -EINVAL);
    EXPECT_EQ(name, dingofs_conf_get(0, "k", NULL, 0),        -EINVAL);
    EXPECT_EQ(name, dingofs_mount(0, "addr", "fs", "/mnt"),   -EINVAL);
    EXPECT_EQ(name, dingofs_umount(0),                         -EINVAL);

    EXPECT_EQ(name, dingofs_open(0, "/f", O_RDONLY, 0),       -EINVAL);
    EXPECT_LT(name, dingofs_read(0, 1, NULL, 0),               0);
    EXPECT_LT(name, dingofs_write(0, 1, NULL, 0),              0);
    EXPECT_LT(name, dingofs_pread(0, 1, NULL, 0, 0),           0);
    EXPECT_LT(name, dingofs_pwrite(0, 1, NULL, 0, 0),          0);
    EXPECT_LT(name, (long)dingofs_lseek(0, 1, 0, SEEK_SET),   0);
    EXPECT_EQ(name, dingofs_flush(0, 1),                       -EINVAL);
    EXPECT_EQ(name, dingofs_fsync(0, 1, 0),                    -EINVAL);
    EXPECT_EQ(name, dingofs_close(0, 1),                       -EINVAL);
    EXPECT_EQ(name, dingofs_ftruncate(0, 1, 0),                -EINVAL);

    struct stat st;
    EXPECT_EQ(name, dingofs_stat(0, "/f", &st),                -EINVAL);
    EXPECT_EQ(name, dingofs_lstat(0, "/f", &st),               -EINVAL);
    EXPECT_EQ(name, dingofs_fstat(0, 1, &st),                  -EINVAL);
    EXPECT_EQ(name, dingofs_chmod(0, "/f", 0644),              -EINVAL);
    EXPECT_EQ(name, dingofs_chown(0, "/f", 0, 0),              -EINVAL);
    EXPECT_EQ(name, dingofs_truncate(0, "/f", 0),              -EINVAL);
    EXPECT_EQ(name, dingofs_utimes(0, "/f", NULL),             -EINVAL);
    EXPECT_EQ(name, dingofs_statfs(0, NULL),                   -EINVAL);

    EXPECT_EQ(name, dingofs_mkdir(0, "/d", 0755),              -EINVAL);
    EXPECT_EQ(name, dingofs_mkdirs(0, "/a/b", 0755),           -EINVAL);
    EXPECT_EQ(name, dingofs_rmdir(0, "/d"),                    -EINVAL);

    uint64_t dh = 0;
    EXPECT_EQ(name, dingofs_opendir(0, "/d", &dh),             -EINVAL);

    dingofs_dirent_t buf[4];
    EXPECT_LT(name, dingofs_readdir(0, 1, buf, 4),             0);
    EXPECT_EQ(name, dingofs_closedir(0, 1),                    -EINVAL);

    EXPECT_EQ(name, dingofs_rename(0, "/a", "/b"),             -EINVAL);
    EXPECT_EQ(name, dingofs_unlink(0, "/f"),                   -EINVAL);
    EXPECT_EQ(name, dingofs_link(0, "/a", "/b"),               -EINVAL);
    EXPECT_EQ(name, dingofs_symlink(0, "/t", "/l"),            -EINVAL);
    EXPECT_LT(name, dingofs_readlink(0, "/l", NULL, 0),        0);

    EXPECT_LT(name, dingofs_setxattr(0, "/f", "n", "v", 1, 0), 0);
    EXPECT_LT(name, dingofs_getxattr(0, "/f", "n", NULL, 0),   0);
    EXPECT_LT(name, dingofs_listxattr(0, "/f", NULL, 0),       0);
    EXPECT_EQ(name, dingofs_removexattr(0, "/f", "n"),         -EINVAL);

    PASS(name);
}

/* NULL path arguments must return -EINVAL. */
static void test_null_path() {
    const char* name = "null_path_returns_EINVAL";
    uintptr_t h = dingofs_new();

    EXPECT_EQ(name, dingofs_open(h, NULL, O_RDONLY, 0),          -EINVAL);
    EXPECT_EQ(name, dingofs_stat(h, NULL, NULL),                  -EINVAL);
    EXPECT_EQ(name, dingofs_lstat(h, NULL, NULL),                 -EINVAL);
    EXPECT_EQ(name, dingofs_chmod(h, NULL, 0644),                 -EINVAL);
    EXPECT_EQ(name, dingofs_chown(h, NULL, 0, 0),                 -EINVAL);
    EXPECT_EQ(name, dingofs_truncate(h, NULL, 0),                 -EINVAL);
    EXPECT_EQ(name, dingofs_utimes(h, NULL, NULL),                -EINVAL);
    EXPECT_EQ(name, dingofs_mkdir(h, NULL, 0755),                 -EINVAL);
    EXPECT_EQ(name, dingofs_mkdirs(h, NULL, 0755),                -EINVAL);
    EXPECT_EQ(name, dingofs_rmdir(h, NULL),                       -EINVAL);
    EXPECT_EQ(name, dingofs_rename(h, NULL, "/b"),                -EINVAL);
    EXPECT_EQ(name, dingofs_rename(h, "/a", NULL),                -EINVAL);
    EXPECT_EQ(name, dingofs_unlink(h, NULL),                      -EINVAL);
    EXPECT_EQ(name, dingofs_link(h, NULL, "/b"),                  -EINVAL);
    EXPECT_EQ(name, dingofs_link(h, "/a", NULL),                  -EINVAL);
    EXPECT_EQ(name, dingofs_symlink(h, NULL, "/l"),               -EINVAL);
    EXPECT_EQ(name, dingofs_symlink(h, "/t", NULL),               -EINVAL);
    EXPECT_LT(name, dingofs_setxattr(h, NULL, "n", "v", 1, 0),   0);
    EXPECT_LT(name, dingofs_setxattr(h, "/f", NULL, "v", 1, 0),  0);
    EXPECT_LT(name, dingofs_getxattr(h, NULL, "n", NULL, 0),      0);
    EXPECT_LT(name, dingofs_listxattr(h, NULL, NULL, 0),          0);
    EXPECT_EQ(name, dingofs_removexattr(h, NULL, "n"),            -EINVAL);
    EXPECT_EQ(name, dingofs_removexattr(h, "/f", NULL),           -EINVAL);

    dingofs_delete(h);
    PASS(name);
}

/* conf_set / conf_get round-trip for every well-known key. */
static void test_conf_roundtrip() {
    const char* name = "conf_roundtrip";
    uintptr_t h = dingofs_new();
    char buf[256];

    /* log.dir */
    EXPECT_EQ(name, dingofs_conf_set(h, "log.dir", "/tmp/test"), 0);
    EXPECT_EQ(name, dingofs_conf_get(h, "log.dir", buf, sizeof(buf)), 0);
    if (strcmp(buf, "/tmp/test") != 0) {
        FAIL(name, "log.dir mismatch");
        dingofs_delete(h);
        return;
    }

    /* log.level */
    EXPECT_EQ(name, dingofs_conf_set(h, "log.level", "WARNING"), 0);
    EXPECT_EQ(name, dingofs_conf_get(h, "log.level", buf, sizeof(buf)), 0);
    if (strcmp(buf, "WARNING") != 0) {
        FAIL(name, "log.level mismatch");
        dingofs_delete(h);
        return;
    }

    /* log.v */
    EXPECT_EQ(name, dingofs_conf_set(h, "log.v", "2"), 0);
    EXPECT_EQ(name, dingofs_conf_get(h, "log.v", buf, sizeof(buf)), 0);
    if (strcmp(buf, "2") != 0) {
        FAIL(name, "log.v mismatch");
        dingofs_delete(h);
        return;
    }

    dingofs_delete(h);
    PASS(name);
}

/* conf_get with a too-small buffer must return -ERANGE. */
static void test_conf_erange() {
    const char* name = "conf_erange";
    uintptr_t h = dingofs_new();

    dingofs_conf_set(h, "log.dir", "/tmp/some/long/path");

    char small[3];
    EXPECT_EQ(name,
              dingofs_conf_get(h, "log.dir", small, sizeof(small)),
              -ERANGE);

    dingofs_delete(h);
    PASS(name);
}

/* conf_set with invalid log.v must return -EINVAL. */
static void test_conf_invalid_log_v() {
    const char* name = "conf_invalid_log_v";
    uintptr_t h = dingofs_new();

    EXPECT_EQ(name, dingofs_conf_set(h, "log.v", "notanumber"), -EINVAL);

    dingofs_delete(h);
    PASS(name);
}

/* Multiple independent instances must not interfere. */
static void test_multiple_instances() {
    const char* name = "multiple_instances";

    uintptr_t h1 = dingofs_new();
    uintptr_t h2 = dingofs_new();

    EXPECT_NE(name, h1, h2);

    /* Each instance has its own log.dir — writes must not bleed across. */
    dingofs_conf_set(h1, "log.dir", "/tmp/h1");
    dingofs_conf_set(h2, "log.dir", "/tmp/h2");

    char buf1[64], buf2[64];
    dingofs_conf_get(h1, "log.dir", buf1, sizeof(buf1));
    dingofs_conf_get(h2, "log.dir", buf2, sizeof(buf2));

    if (strcmp(buf1, "/tmp/h1") != 0 || strcmp(buf2, "/tmp/h2") != 0) {
        FAIL(name, "instance config leaked between handles");
        dingofs_delete(h1);
        dingofs_delete(h2);
        return;
    }

    dingofs_delete(h1);
    dingofs_delete(h2);
    PASS(name);
}

/* Operations on an invalid fd must return -EBADF. */
static void test_invalid_fd() {
    const char* name = "invalid_fd";
    uintptr_t h = dingofs_new();

    char buf[16];
    EXPECT_EQ(name, dingofs_read(h, 999, buf, sizeof(buf)),   -EBADF);
    EXPECT_EQ(name, dingofs_write(h, 999, buf, sizeof(buf)),  -EBADF);
    EXPECT_EQ(name, dingofs_flush(h, 999),                    -EBADF);
    EXPECT_EQ(name, dingofs_fsync(h, 999, 0),                 -EBADF);
    EXPECT_EQ(name, dingofs_close(h, 999),                    -EBADF);
    EXPECT_EQ(name, dingofs_ftruncate(h, 999, 0),             -EBADF);

    struct stat st;
    EXPECT_EQ(name, dingofs_fstat(h, 999, &st),               -EBADF);
    EXPECT_LT(name, (int)dingofs_lseek(h, 999, 0, SEEK_SET),  0);

    dingofs_delete(h);
    PASS(name);
}

/* Operations on an invalid dh must return -EBADF. */
static void test_invalid_dh() {
    const char* name = "invalid_dh";
    uintptr_t h = dingofs_new();

    dingofs_dirent_t buf[4];
    EXPECT_EQ(name, dingofs_readdir(h, 999, buf, 4),   -EBADF);
    EXPECT_EQ(name, dingofs_closedir(h, 999),           -EBADF);

    dingofs_delete(h);
    PASS(name);
}

/* readdir with count ≤ 0 must return -EINVAL. */
static void test_readdir_invalid_count() {
    const char* name = "readdir_invalid_count";
    uintptr_t h = dingofs_new();

    dingofs_dirent_t buf[4];
    EXPECT_EQ(name, dingofs_readdir(h, 1, buf,  0), -EINVAL);
    EXPECT_EQ(name, dingofs_readdir(h, 1, buf, -1), -EINVAL);

    dingofs_delete(h);
    PASS(name);
}

/* ============================================================================
 * Group 2: cluster-dependent (skipped when DINGOFS_MDS_ADDRS is unset)
 * ============================================================================ */

static void test_io_ops(const char* mds_addrs,
                            const char* fsname,
                            const char* mountpoint) {
    uintptr_t h = dingofs_new();
    dingofs_conf_set(h, "log.dir",   "/tmp/dingofs-test-log");
    dingofs_conf_set(h, "log.level", "WARNING");

    if (dingofs_mount(h, mds_addrs, fsname, mountpoint) != 0) {
        SKIP("io_ops", "mount failed — cluster may not be running");
        dingofs_delete(h);
        return;
    }

    const char* dir  = "/libdingofs_test_dir";
    const char* file = "/libdingofs_test_dir/data.bin";

    /* Declare all locals before any goto targets. */
    const char* payload = "dingofs-c-api-test";
    int         fd      = -1;
    ssize_t     wn, rn, lr, xn;
    struct stat st;
    char        rbuf[64];
    char        pbuf[4];
    char        newpath[128];
    char        lpath[128];
    char        lbuf[256];
    char        xbuf[16];
    uint64_t    dh  = 0;
    int         total = 0, n = 0;
    dingofs_dirent_t dbuf[16];

#define CK(expr, label)                                                 \
    do {                                                                \
        int _r = (int)(expr);                                           \
        if (_r < 0) {                                                   \
            printf("[ FAIL ] io_ops/%s  errno=%d (%s)\n",          \
                   (label), -_r, strerror(-_r));                        \
            ++g_fail;                                                   \
            dingofs_umount(h); dingofs_delete(h); return;              \
        }                                                               \
    } while (0)

    /* mkdir */
    CK(dingofs_mkdir(h, dir, 0755), "mkdir");

    /* write */
    fd = dingofs_open(h, file, O_WRONLY | O_CREAT, 0644);
    CK(fd, "open(write)");
    wn = dingofs_write(h, fd, payload, strlen(payload));
    if (wn != (ssize_t)strlen(payload)) {
        printf("[ FAIL ] io_ops/write  wrote %zd expected %zu\n",
               wn, strlen(payload));
        ++g_fail;
        dingofs_close(h, fd);
        goto cleanup;
    }
    CK(dingofs_close(h, fd), "close(write)");

    /* stat */
    CK(dingofs_stat(h, file, &st), "stat");
    if (st.st_size != (off_t)strlen(payload)) {
        printf("[ FAIL ] io_ops/stat_size  expected %zu got %ld\n",
               strlen(payload), (long)st.st_size);
        ++g_fail;
        goto cleanup;
    }

    /* read */
    fd = dingofs_open(h, file, O_RDONLY, 0);
    CK(fd, "open(read)");
    memset(rbuf, 0, sizeof(rbuf));
    rn = dingofs_read(h, fd, rbuf, sizeof(rbuf) - 1);
    if (rn != (ssize_t)strlen(payload) ||
        memcmp(rbuf, payload, strlen(payload)) != 0) {
        printf("[ FAIL ] io_ops/read  got %zd bytes: \"%s\"\n", rn, rbuf);
        ++g_fail;
        dingofs_close(h, fd);
        goto cleanup;
    }
    CK(dingofs_close(h, fd), "close(read)");

    /* pwrite / pread */
    fd = dingofs_open(h, file, O_RDWR, 0);
    CK(fd, "open(rdwr)");
    CK(dingofs_pwrite(h, fd, "XX", 2, 0), "pwrite");
    memset(pbuf, 0, sizeof(pbuf));
    CK(dingofs_pread(h, fd, pbuf, 2, 0), "pread");
    if (memcmp(pbuf, "XX", 2) != 0) {
        printf("[ FAIL ] io_ops/pread  got %.2s\n", pbuf);
        ++g_fail;
    }
    CK(dingofs_close(h, fd), "close(rdwr)");

    /* truncate */
    CK(dingofs_truncate(h, file, 4), "truncate");
    CK(dingofs_stat(h, file, &st), "stat after truncate");
    if (st.st_size != 4) {
        printf("[ FAIL ] io_ops/truncate_size  expected 4 got %ld\n",
               (long)st.st_size);
        ++g_fail;
    }

    /* rename */
    snprintf(newpath, sizeof(newpath), "%s/renamed.bin", dir);
    CK(dingofs_rename(h, file, newpath), "rename");

    /* symlink / readlink */
    snprintf(lpath, sizeof(lpath), "%s/link.bin", dir);
    CK(dingofs_symlink(h, newpath, lpath), "symlink");
    memset(lbuf, 0, sizeof(lbuf));
    lr = dingofs_readlink(h, lpath, lbuf, sizeof(lbuf) - 1);
    if (lr < 0) {
        printf("[ FAIL ] io_ops/readlink  errno=%d\n", (int)-lr);
        ++g_fail;
    }

    /* xattr */
    CK(dingofs_setxattr(h, newpath, "user.k", "val", 3, 0), "setxattr");
    memset(xbuf, 0, sizeof(xbuf));
    xn = dingofs_getxattr(h, newpath, "user.k", xbuf, sizeof(xbuf));
    if (xn != 3 || memcmp(xbuf, "val", 3) != 0) {
        printf("[ FAIL ] io_ops/getxattr  got %zd \"%.*s\"\n",
               xn, (int)xn, xbuf);
        ++g_fail;
    }
    CK(dingofs_removexattr(h, newpath, "user.k"), "removexattr");

    /* opendir / readdir */
    CK(dingofs_opendir(h, dir, &dh), "opendir");
    while ((n = dingofs_readdir(h, dh, dbuf, 16)) > 0) total += n;
    CK(n, "readdir");    /* n == 0 means EOF, which is fine */
    (void)total;
    CK(dingofs_closedir(h, dh), "closedir");

cleanup:
    /* clean up regardless of intermediate failures */
    dingofs_unlink(h, lpath);
    dingofs_unlink(h, newpath);
    dingofs_unlink(h, file);   /* in case rename was skipped */
    dingofs_rmdir(h,  dir);

    dingofs_umount(h);
    dingofs_delete(h);

    if (g_fail == 0)
        PASS("io_ops");

#undef CK
}

/* ============================================================================
 * Entry point
 * ============================================================================ */

int main(void) {
    printf("=== libdingofs unit tests ===\n\n");

    /* Group 1: no cluster */
    test_new_delete();
    test_delete_null();
    test_null_handle();
    test_null_path();
    test_conf_roundtrip();
    test_conf_erange();
    test_conf_invalid_log_v();
    test_multiple_instances();
    test_invalid_fd();
    test_invalid_dh();
    test_readdir_invalid_count();

    /* Group 2: I/O tests — always run against the in-memory mock.
     * When DINGOFS_MDS_ADDRS / DINGOFS_FS_NAME / DINGOFS_MOUNT_POINT are
     * set the same function is exercised against a real cluster. */
    const char* mds_addrs  = getenv("DINGOFS_MDS_ADDRS");
    const char* fsname     = getenv("DINGOFS_FS_NAME");
    const char* mountpoint = getenv("DINGOFS_MOUNT_POINT");

    test_io_ops(
        mds_addrs  ? mds_addrs  : "mock",
        fsname     ? fsname     : "mockfs",
        mountpoint ? mountpoint : "/mock"
    );

    printf("\n=== Results: %d passed, %d failed, %d skipped ===\n",
           g_pass, g_fail, g_skip);
    return g_fail > 0 ? 1 : 0;
}
