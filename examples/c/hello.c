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
 * hello.c — libdingofs C API walkthrough
 *
 * Demonstrates:
 *   - mount / umount
 *   - mkdir / rmdir
 *   - create / write / read / close
 *   - stat / chmod
 *   - rename / unlink
 *   - symlink / readlink
 *   - opendir / readdir / closedir
 *   - setxattr / getxattr / listxattr / removexattr
 *
 * Usage:
 *   ./hello <mds_addrs> <fsname> <mountpoint>
 *
 * Example:
 *   ./hello "10.0.0.1:8801" myfs /mnt/myfs
 */

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

#include "dingofs/libdingofs.h"

/* --------------------------------------------------------------------------
 * Helpers
 * -------------------------------------------------------------------------- */

static uintptr_t g_h = 0;

/* Print OK / FAIL and abort on error. */
#define DINGOFS_CHECK(expr, label)                                         \
  do {                                                                     \
    int _r = (int)(expr);                                                  \
    if (_r < 0) {                                                          \
      (void)fprintf(stderr, "[FAIL] %-30s  errno=%d (%s)\n", (label), -_r, \
                    strerror(-_r));                                        \
      dingofs_umount(g_h);                                                 \
      dingofs_delete(g_h);                                                 \
      exit(1);                                                             \
    }                                                                      \
    (void)printf("[ OK ] %s\n", (label));                                  \
  } while (0)

/* Like DINGOFS_CHECK but for functions returning an fd (≥ 0). */
#define DINGOFS_CHECK_FD(fd_var, expr, label)                         \
  do {                                                                \
    (fd_var) = (expr);                                                \
    if ((fd_var) < 0) {                                               \
      (void)fprintf(stderr, "[FAIL] %-30s  errno=%d (%s)\n", (label), \
                    -(fd_var), strerror(-(fd_var)));                  \
      dingofs_umount(g_h);                                            \
      dingofs_delete(g_h);                                            \
      exit(1);                                                        \
    }                                                                 \
    (void)printf("[ OK ] %s  fd=%d\n", (label), (fd_var));            \
  } while (0)

/* ============================================================================
 * main
 * ============================================================================
 */

int main(int argc, char* argv[]) {
  if (argc < 4) {
    (void)fprintf(stderr,
                  "Usage: %s <mds_addrs> <fsname> <mountpoint>\n"
                  "Example: %s 10.0.0.1:8801 myfs /mnt/myfs\n",
                  argv[0], argv[0]);
    return 1;
  }

  const char* mds_addrs = argv[1];
  const char* fsname = argv[2];
  const char* mountpoint = argv[3];

  /* ---- lifecycle ---- */
  g_h = dingofs_new();

  (void)dingofs_conf_set(g_h, "log.dir", "/tmp/dingofs-hello-log");
  (void)dingofs_conf_set(g_h, "log.level", "WARNING");

  DINGOFS_CHECK(dingofs_mount(g_h, mds_addrs, fsname, mountpoint), "mount");

  /* ---- statfs ---- */
  dingofs_statfs_t fsstat;
  DINGOFS_CHECK(dingofs_statfs(g_h, &fsstat), "statfs");
  (void)printf("       total=%.1f GB  used=%.1f GB\n", fsstat.total_bytes / 1e9,
               fsstat.used_bytes / 1e9);

  /* ---- mkdir ---- */
  DINGOFS_CHECK(dingofs_mkdir(g_h, "/hello_c", 0755), "mkdir /hello_c");

  /* ---- stat ---- */
  struct stat st;
  DINGOFS_CHECK(dingofs_stat(g_h, "/hello_c", &st), "stat /hello_c");
  (void)printf("       ino=%lu  mode=%04o\n", (unsigned long)st.st_ino,
               st.st_mode & 07777);

  /* ---- chmod ---- */
  DINGOFS_CHECK(dingofs_chmod(g_h, "/hello_c", 0700), "chmod 700 /hello_c");

  /* ---- create + write ---- */
  const char* filepath = "/hello_c/world.txt";
  const char* payload = "Hello from DingoFS C API!\n";

  int fd;
  DINGOFS_CHECK_FD(fd, dingofs_open(g_h, filepath, O_WRONLY | O_CREAT, 0644),
                   "open(O_WRONLY|O_CREAT)");

  ssize_t wn = dingofs_write(g_h, fd, payload, strlen(payload));
  if (wn < 0) {
    (void)fprintf(stderr, "[FAIL] write  errno=%d (%s)\n", (int)-wn,
                  strerror((int)-wn));
    exit(1);
  }
  (void)printf("[ OK ] write  %zd bytes\n", wn);

  DINGOFS_CHECK(dingofs_flush(g_h, fd), "flush");
  DINGOFS_CHECK(dingofs_fsync(g_h, fd, 0), "fsync");
  DINGOFS_CHECK(dingofs_close(g_h, fd), "close(write fd)");

  /* ---- stat after write ---- */
  DINGOFS_CHECK(dingofs_stat(g_h, filepath, &st), "stat after write");
  (void)printf("       size=%ld bytes\n", (long)st.st_size);

  /* ---- open + read ---- */
  DINGOFS_CHECK_FD(fd, dingofs_open(g_h, filepath, O_RDONLY, 0),
                   "open(O_RDONLY)");

  char rbuf[256] = {0};
  ssize_t rn = dingofs_read(g_h, fd, rbuf, sizeof(rbuf) - 1);
  if (rn < 0) {
    (void)fprintf(stderr, "[FAIL] read  errno=%d (%s)\n", (int)-rn,
                  strerror((int)-rn));
    exit(1);
  }
  (void)printf("[ OK ] read   %zd bytes: %s", rn, rbuf);

  /* ---- lseek + pread ---- */
  off_t new_pos = dingofs_lseek(g_h, fd, 6, SEEK_SET);
  if (new_pos < 0) {
    (void)fprintf(stderr, "[FAIL] lseek  errno=%d\n", (int)-new_pos);
    exit(1);
  }
  (void)printf("[ OK ] lseek  pos=%ld\n", (long)new_pos);

  char pbuf[16] = {0};
  ssize_t pn =
      dingofs_pread(g_h, fd, pbuf, 4, 0); /* read "Hell" from offset 0 */
  if (pn < 0) {
    (void)fprintf(stderr, "[FAIL] pread  errno=%d\n", (int)-pn);
    exit(1);
  }
  (void)printf("[ OK ] pread  4 bytes at offset 0: \"%.4s\"\n", pbuf);

  DINGOFS_CHECK(dingofs_close(g_h, fd), "close(read fd)");

  /* ---- truncate ---- */
  DINGOFS_CHECK(dingofs_truncate(g_h, filepath, 5), "truncate to 5 bytes");
  DINGOFS_CHECK(dingofs_stat(g_h, filepath, &st), "stat after truncate");
  (void)printf("       size=%ld bytes\n", (long)st.st_size);

  /* ---- xattr ---- */
  DINGOFS_CHECK(dingofs_setxattr(g_h, filepath, "user.tag", "demo", 4, 0),
                "setxattr user.tag=demo");

  char xval[64] = {0};
  ssize_t xn = dingofs_getxattr(g_h, filepath, "user.tag", xval, sizeof(xval));
  if (xn < 0) {
    (void)fprintf(stderr, "[FAIL] getxattr  errno=%d\n", (int)-xn);
    exit(1);
  }
  (void)printf("[ OK ] getxattr  user.tag=%.*s\n", (int)xn, xval);

  char xlist[256] = {0};
  ssize_t ln = dingofs_listxattr(g_h, filepath, xlist, sizeof(xlist));
  if (ln >= 0) {
    (void)printf("[ OK ] listxattr  %zd bytes\n", ln);
  }

  DINGOFS_CHECK(dingofs_removexattr(g_h, filepath, "user.tag"), "removexattr");

  /* ---- rename ---- */
  DINGOFS_CHECK(dingofs_rename(g_h, filepath, "/hello_c/renamed.txt"),
                "rename world.txt → renamed.txt");

  /* ---- symlink + readlink ---- */
  DINGOFS_CHECK(
      dingofs_symlink(g_h, "/hello_c/renamed.txt", "/hello_c/link.txt"),
      "symlink link.txt → renamed.txt");

  char lbuf[256] = {0};
  ssize_t lr =
      dingofs_readlink(g_h, "/hello_c/link.txt", lbuf, sizeof(lbuf) - 1);
  if (lr < 0) {
    (void)fprintf(stderr, "[FAIL] readlink  errno=%d\n", (int)-lr);
    exit(1);
  }
  lbuf[lr] = '\0';
  (void)printf("[ OK ] readlink  → %s\n", lbuf);

  /* ---- opendir / readdir / closedir ---- */
  uint64_t dh = 0;
  DINGOFS_CHECK(dingofs_opendir(g_h, "/hello_c", &dh), "opendir /hello_c");
  (void)printf("[ OK ] opendir  dh=%llu\n", (unsigned long long)dh);

  dingofs_dirent_t entries[16];
  int n;
  while ((n = dingofs_readdir(g_h, dh, entries, 16)) > 0) {
    for (int i = 0; i < n; i++) {
      (void)printf("       %-20s  ino=%-8llu  type=%u\n", entries[i].d_name,
                   (unsigned long long)entries[i].d_ino, entries[i].d_type);
    }
  }
  if (n < 0) {
    (void)fprintf(stderr, "[FAIL] readdir  errno=%d\n", -n);
  } else {
    (void)printf("[ OK ] readdir done\n");
  }
  DINGOFS_CHECK(dingofs_closedir(g_h, dh), "closedir");

  /* ---- cleanup ---- */
  DINGOFS_CHECK(dingofs_unlink(g_h, "/hello_c/link.txt"), "unlink link.txt");
  DINGOFS_CHECK(dingofs_unlink(g_h, "/hello_c/renamed.txt"),
                "unlink renamed.txt");
  DINGOFS_CHECK(dingofs_rmdir(g_h, "/hello_c"), "rmdir /hello_c");

  /* ---- umount ---- */
  DINGOFS_CHECK(dingofs_umount(g_h), "umount");
  dingofs_delete(g_h);

  (void)printf("\nAll operations succeeded.\n");
  return 0;
}
