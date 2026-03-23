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
 * mknod — create a regular file node (O_CREAT without O_RDWR write-access).
 * The C API does not expose a raw mknod(2) syscall; creating a regular-file
 * inode is done via dingofs_open(O_CREAT) + immediate close.
 */

#include <fcntl.h>
#include <stdio.h>

#include "common.h"

int main(void) {
  uintptr_t h = make_client();
  DINGOFS_CHECK(dingofs_mount(h, MDS_ADDRS, FS_NAME, MOUNT_POINT), "mount");

  /* setup: create parent dir */
  DINGOFS_CHECK(dingofs_mkdir(h, "/demo_dir", 0755), "mkdir");

  /* mknod: create an empty regular-file inode */
  int fd = dingofs_open(h, "/demo_dir/demo_node", O_CREAT | O_WRONLY, 0644);
  if (fd < 0) {
    (void)fprintf(stderr, "[FAIL] mknod (open O_CREAT): %s\n", strerror(-fd));
    return 1;
  }
  DINGOFS_CHECK(dingofs_close(h, fd), "close");

  /* verify */
  struct stat st = {0};
  DINGOFS_CHECK(dingofs_stat(h, "/demo_dir/demo_node", &st), "stat");
  (void)printf("ino:  %llu\n", (unsigned long long)st.st_ino);
  (void)printf("mode: 0%o\n", (unsigned)st.st_mode & 07777);

  /* cleanup */
  DINGOFS_CHECK(dingofs_unlink(h, "/demo_dir/demo_node"), "unlink");
  DINGOFS_CHECK(dingofs_rmdir(h, "/demo_dir"), "rmdir");

  DINGOFS_CHECK(dingofs_umount(h), "umount");
  dingofs_delete(h);
  return 0;
}
