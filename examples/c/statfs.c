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

  dingofs_statfs_t st = {0};
  DINGOFS_CHECK(dingofs_statfs(h, &st), "statfs");

  (void)printf("total_bytes:   %lld\n", (long long)st.total_bytes);
  (void)printf("used_bytes:    %lld\n", (long long)st.used_bytes);
  (void)printf("total_inodes:  %lld\n", (long long)st.total_inodes);
  (void)printf("used_inodes:   %lld\n", (long long)st.used_inodes);

  DINGOFS_CHECK(dingofs_umount(h), "umount");
  dingofs_delete(h);
  return 0;
}
