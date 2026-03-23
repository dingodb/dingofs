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

  int fd = dingofs_open(h, "/demo_dir/hello.txt", O_CREAT | O_RDWR, 0644);
  if (fd < 0) {
    (void)fprintf(stderr, "[FAIL] open: %s\n", strerror(-fd));
    return 1;
  }
  DINGOFS_CHECK(dingofs_close(h, fd), "close");

  /* stat */
  struct stat st = {0};
  DINGOFS_CHECK(dingofs_stat(h, "/demo_dir/hello.txt", &st), "stat");
  (void)printf("size:  %lld\n", (long long)st.st_size);
  (void)printf("mode:  0%o\n", (unsigned)st.st_mode & 07777);
  (void)printf("ino:   %llu\n", (unsigned long long)st.st_ino);

  /* chmod 0600 via chmod */
  DINGOFS_CHECK(dingofs_chmod(h, "/demo_dir/hello.txt", 0600), "chmod 0600");

  /* verify new mode */
  struct stat st2 = {0};
  DINGOFS_CHECK(dingofs_stat(h, "/demo_dir/hello.txt", &st2),
                "stat after chmod");
  (void)printf("new mode: 0%o\n", (unsigned)st2.st_mode & 07777);

  /* fstat via open fd */
  int fd2 = dingofs_open(h, "/demo_dir/hello.txt", O_RDONLY, 0);
  if (fd2 < 0) {
    (void)fprintf(stderr, "[FAIL] open for fstat: %s\n", strerror(-fd2));
    return 1;
  }
  struct stat st3 = {0};
  DINGOFS_CHECK(dingofs_fstat(h, fd2, &st3), "fstat");
  (void)printf("fstat mode: 0%o\n", (unsigned)st3.st_mode & 07777);
  DINGOFS_CHECK(dingofs_close(h, fd2), "close fd2");

  /* cleanup */
  DINGOFS_CHECK(dingofs_unlink(h, "/demo_dir/hello.txt"), "unlink");
  DINGOFS_CHECK(dingofs_rmdir(h, "/demo_dir"), "rmdir");

  DINGOFS_CHECK(dingofs_umount(h), "umount");
  dingofs_delete(h);
  return 0;
}
