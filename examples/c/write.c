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

  /* setup: create parent dir */
  DINGOFS_CHECK(dingofs_mkdir(h, "/demo_dir", 0755), "mkdir");

  /* create file */
  int fd = dingofs_open(h, "/demo_dir/hello.txt", O_CREAT | O_RDWR, 0644);
  if (fd < 0) {
    (void)fprintf(stderr, "[FAIL] open: %s\n", strerror(-fd));
    return 1;
  }
  (void)printf("[ OK ] open  fd=%d\n", fd);

  /* write */
  const char* data = "Hello DingoFS C SDK!";
  ssize_t wn = dingofs_write(h, fd, data, strlen(data));
  if (wn < 0) {
    (void)fprintf(stderr, "[FAIL] write: %s\n", strerror((int)-wn));
    return 1;
  }
  (void)printf("[ OK ] write  %zd bytes\n", wn);

  DINGOFS_CHECK(dingofs_flush(h, fd), "flush");
  DINGOFS_CHECK(dingofs_fsync(h, fd, 0), "fsync");
  DINGOFS_CHECK(dingofs_close(h, fd), "close");

  /* cleanup */
  DINGOFS_CHECK(dingofs_unlink(h, "/demo_dir/hello.txt"), "unlink");
  DINGOFS_CHECK(dingofs_rmdir(h, "/demo_dir"), "rmdir");

  DINGOFS_CHECK(dingofs_umount(h), "umount");
  dingofs_delete(h);
  return 0;
}
