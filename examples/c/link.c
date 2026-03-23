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

  /* setup: create dir + original file */
  DINGOFS_CHECK(dingofs_mkdir(h, "/demo_dir", 0755), "mkdir");

  int fd = dingofs_open(h, "/demo_dir/original.txt", O_CREAT | O_RDWR, 0644);
  if (fd < 0) {
    (void)fprintf(stderr, "[FAIL] open: %s\n", strerror(-fd));
    return 1;
  }
  DINGOFS_CHECK(dingofs_close(h, fd), "close");

  /* hard link */
  DINGOFS_CHECK(
      dingofs_link(h, "/demo_dir/original.txt", "/demo_dir/hardlink.txt"),
      "link");

  /* verify: both paths share the same inode */
  struct stat st1 = {0}, st2 = {0};
  DINGOFS_CHECK(dingofs_stat(h, "/demo_dir/original.txt", &st1),
                "stat original");
  DINGOFS_CHECK(dingofs_stat(h, "/demo_dir/hardlink.txt", &st2),
                "stat hardlink");
  (void)printf("original  ino=%llu  nlink=%lu\n",
               (unsigned long long)st1.st_ino, (unsigned long)st1.st_nlink);
  (void)printf("hardlink  ino=%llu  nlink=%lu\n",
               (unsigned long long)st2.st_ino, (unsigned long)st2.st_nlink);

  if (st1.st_ino == st2.st_ino)
    (void)printf("[ OK ] same inode verified\n");
  else {
    (void)fprintf(stderr, "[FAIL] ino mismatch\n");
    return 1;
  }

  DINGOFS_CHECK(dingofs_unlink(h, "/demo_dir/hardlink.txt"), "unlink hardlink");

  /* cleanup */
  DINGOFS_CHECK(dingofs_unlink(h, "/demo_dir/original.txt"), "unlink original");
  DINGOFS_CHECK(dingofs_rmdir(h, "/demo_dir"), "rmdir");

  DINGOFS_CHECK(dingofs_umount(h), "umount");
  dingofs_delete(h);
  return 0;
}
