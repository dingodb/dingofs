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

#include <fcntl.h>
#include <stdio.h>
#include <string.h>

#include "common.h"

int main(void) {
  uintptr_t h = make_client();
  DINGOFS_CHECK(dingofs_mount(h, MDS_ADDRS, FS_NAME, MOUNT_POINT), "mount");

  /* setup: create dir + target file */
  DINGOFS_CHECK(dingofs_mkdir(h, "/demo_dir", 0755), "mkdir");

  int fd = dingofs_open(h, "/demo_dir/target.txt", O_CREAT | O_RDWR, 0644);
  if (fd < 0) {
    (void)fprintf(stderr, "[FAIL] open target: %s\n", strerror(-fd));
    return 1;
  }
  DINGOFS_CHECK(dingofs_close(h, fd), "close target");

  /* symlink: link.txt → target absolute path */
  const char* target = MOUNT_POINT "/demo_dir/target.txt";
  const char* linkpath = "/demo_dir/link.txt";
  DINGOFS_CHECK(dingofs_symlink(h, target, linkpath), "symlink");

  /* lstat: link itself (should be a symlink) */
  struct stat lst = {0};
  DINGOFS_CHECK(dingofs_lstat(h, linkpath, &lst), "lstat link");
  (void)printf("link ino:  %llu\n", (unsigned long long)lst.st_ino);

  /* readlink */
  char buf[512] = {0};
  ssize_t rn = dingofs_readlink(h, linkpath, buf, sizeof(buf) - 1);
  if (rn < 0) {
    (void)fprintf(stderr, "[FAIL] readlink: %s\n", strerror((int)-rn));
    return 1;
  }
  buf[rn] = '\0';
  (void)printf("[ OK ] readlink\n");
  (void)printf("target: %s\n", buf);

  /* verify */
  if (strcmp(buf, target) != 0) {
    (void)fprintf(stderr, "[FAIL] target mismatch\n");
    return 1;
  }
  (void)printf("[ OK ] target verified\n");

  /* cleanup */
  DINGOFS_CHECK(dingofs_unlink(h, linkpath), "unlink link");
  DINGOFS_CHECK(dingofs_unlink(h, "/demo_dir/target.txt"), "unlink target");
  DINGOFS_CHECK(dingofs_rmdir(h, "/demo_dir"), "rmdir");

  DINGOFS_CHECK(dingofs_umount(h), "umount");
  dingofs_delete(h);
  return 0;
}
