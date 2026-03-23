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

#include <stdio.h>

#include "common.h"

int main(void) {
  uintptr_t h = make_client();
  DINGOFS_CHECK(dingofs_mount(h, MDS_ADDRS, FS_NAME, MOUNT_POINT), "mount");

  /* mkdir */
  DINGOFS_CHECK(dingofs_mkdir(h, "/demo_dir", 0755), "mkdir /demo_dir");

  /* stat the new directory */
  struct stat st = {0};
  DINGOFS_CHECK(dingofs_stat(h, "/demo_dir", &st), "stat /demo_dir");
  (void)printf("ino: %llu\n", (unsigned long long)st.st_ino);

  /* opendir / readdir / closedir */
  uint64_t dh = 0;
  DINGOFS_CHECK(dingofs_opendir(h, "/demo_dir", &dh), "opendir");

  dingofs_dirent_t entries[32];
  int n;
  (void)printf("entries:\n");
  while ((n = dingofs_readdir(h, dh, entries, 32)) > 0) {
    for (int i = 0; i < n; i++)
      (void)printf("  %s  ino=%llu\n", entries[i].d_name,
                   (unsigned long long)entries[i].d_ino);
  }
  if (n < 0) {
    (void)fprintf(stderr, "[FAIL] readdir: %s\n", strerror(-n));
    return 1;
  }
  (void)printf("[ OK ] readdir\n");

  DINGOFS_CHECK(dingofs_closedir(h, dh), "closedir");

  /* cleanup */
  DINGOFS_CHECK(dingofs_rmdir(h, "/demo_dir"), "rmdir");

  DINGOFS_CHECK(dingofs_umount(h), "umount");
  dingofs_delete(h);
  return 0;
}
