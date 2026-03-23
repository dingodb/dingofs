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

#include "common.h"

int main(void) {
  uintptr_t h = make_client();
  DINGOFS_CHECK(dingofs_mount(h, MDS_ADDRS, FS_NAME, MOUNT_POINT), "mount");

  /* setup: create dir + file */
  DINGOFS_CHECK(dingofs_mkdir(h, "/demo_dir", 0755), "mkdir");

  int fd = dingofs_open(h, "/demo_dir/old_name.txt", O_CREAT | O_RDWR, 0644);
  if (fd < 0) {
    (void)fprintf(stderr, "[FAIL] open: %s\n", strerror(-fd));
    return 1;
  }
  struct stat st0 = {0};
  DINGOFS_CHECK(dingofs_fstat(h, fd, &st0), "fstat before rename");
  (void)printf("ino before rename: %llu\n", (unsigned long long)st0.st_ino);
  DINGOFS_CHECK(dingofs_close(h, fd), "close");

  /* rename */
  DINGOFS_CHECK(
      dingofs_rename(h, "/demo_dir/old_name.txt", "/demo_dir/new_name.txt"),
      "rename");

  /* verify: new name exists, same inode */
  struct stat st1 = {0};
  DINGOFS_CHECK(dingofs_stat(h, "/demo_dir/new_name.txt", &st1),
                "stat after rename");
  (void)printf("ino after rename:  %llu\n", (unsigned long long)st1.st_ino);

  if (st0.st_ino == st1.st_ino)
    (void)printf("[ OK ] same inode verified\n");
  else {
    (void)fprintf(stderr, "[FAIL] ino mismatch\n");
    return 1;
  }

  /* cleanup */
  DINGOFS_CHECK(dingofs_unlink(h, "/demo_dir/new_name.txt"), "unlink");
  DINGOFS_CHECK(dingofs_rmdir(h, "/demo_dir"), "rmdir");

  DINGOFS_CHECK(dingofs_umount(h), "umount");
  dingofs_delete(h);
  return 0;
}
