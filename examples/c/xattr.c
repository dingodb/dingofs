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

  /* setup: create dir + file */
  DINGOFS_CHECK(dingofs_mkdir(h, "/demo_dir", 0755), "mkdir");

  int fd = dingofs_open(h, "/demo_dir/hello.txt", O_CREAT | O_RDWR, 0644);
  if (fd < 0) {
    (void)fprintf(stderr, "[FAIL] open: %s\n", strerror(-fd));
    return 1;
  }
  DINGOFS_CHECK(dingofs_close(h, fd), "close");

  const char* path = "/demo_dir/hello.txt";

  /* setxattr */
  DINGOFS_CHECK(dingofs_setxattr(h, path, "user.author", "dingodb", 7, 0),
                "setxattr user.author");
  DINGOFS_CHECK(dingofs_setxattr(h, path, "user.version", "1.0", 3, 0),
                "setxattr user.version");

  /* getxattr */
  char val[256] = {0};
  ssize_t vsz = dingofs_getxattr(h, path, "user.author", val, sizeof(val));
  if (vsz < 0) {
    (void)fprintf(stderr, "[FAIL] getxattr: %s\n", strerror((int)-vsz));
    return 1;
  }
  val[vsz] = '\0';
  (void)printf("[ OK ] getxattr user.author\n");
  (void)printf("user.author = %s\n", val);

  /* listxattr */
  char list[1024] = {0};
  ssize_t lsz = dingofs_listxattr(h, path, list, sizeof(list));
  if (lsz < 0) {
    (void)fprintf(stderr, "[FAIL] listxattr: %s\n", strerror((int)-lsz));
    return 1;
  }
  (void)printf("[ OK ] listxattr\n");
  (void)printf("xattrs:\n");
  const char* p = list;
  while (p < list + lsz) {
    (void)printf("  %s\n", p);
    p += strlen(p) + 1;
  }

  /* removexattr */
  DINGOFS_CHECK(dingofs_removexattr(h, path, "user.author"),
                "removexattr user.author");
  DINGOFS_CHECK(dingofs_removexattr(h, path, "user.version"),
                "removexattr user.version");

  /* cleanup */
  DINGOFS_CHECK(dingofs_unlink(h, "/demo_dir/hello.txt"), "unlink");
  DINGOFS_CHECK(dingofs_rmdir(h, "/demo_dir"), "rmdir");

  DINGOFS_CHECK(dingofs_umount(h), "umount");
  dingofs_delete(h);
  return 0;
}
