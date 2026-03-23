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

  /* setup: create dir + file + write content */
  DINGOFS_CHECK(dingofs_mkdir(h, "/demo_dir", 0755), "mkdir");

  int wfd = dingofs_open(h, "/demo_dir/hello.txt", O_CREAT | O_RDWR, 0644);
  if (wfd < 0) {
    (void)fprintf(stderr, "[FAIL] open(write): %s\n", strerror(-wfd));
    return 1;
  }

  const char* data = "Hello DingoFS C SDK!";
  const size_t dlen = strlen(data);
  ssize_t wn = dingofs_write(h, wfd, data, dlen);
  if (wn < 0) {
    (void)fprintf(stderr, "[FAIL] write: %s\n", strerror((int)-wn));
    return 1;
  }
  DINGOFS_CHECK(dingofs_flush(h, wfd), "flush");
  DINGOFS_CHECK(dingofs_close(h, wfd), "close(write)");

  /* read back */
  int rfd = dingofs_open(h, "/demo_dir/hello.txt", O_RDONLY, 0);
  if (rfd < 0) {
    (void)fprintf(stderr, "[FAIL] open(read): %s\n", strerror(-rfd));
    return 1;
  }
  (void)printf("[ OK ] open(read)  fd=%d\n", rfd);

  char buf[256] = {0};
  ssize_t rn = dingofs_read(h, rfd, buf, sizeof(buf) - 1);
  if (rn < 0) {
    (void)fprintf(stderr, "[FAIL] read: %s\n", strerror((int)-rn));
    return 1;
  }
  (void)printf("[ OK ] read  %zd bytes\n", rn);
  (void)printf("content: [%s]\n", buf);

  /* verify */
  if ((size_t)rn != dlen || memcmp(buf, data, dlen) != 0) {
    (void)fprintf(stderr, "[FAIL] content mismatch\n");
    return 1;
  }
  (void)printf("[ OK ] content verified\n");

  DINGOFS_CHECK(dingofs_close(h, rfd), "close(read)");

  /* cleanup */
  DINGOFS_CHECK(dingofs_unlink(h, "/demo_dir/hello.txt"), "unlink");
  DINGOFS_CHECK(dingofs_rmdir(h, "/demo_dir"), "rmdir");

  DINGOFS_CHECK(dingofs_umount(h), "umount");
  dingofs_delete(h);
  return 0;
}
