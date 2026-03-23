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

// statfs.cc — query filesystem-level usage statistics via the C API.

#include <iostream>

#include "common.h"

int main() {
  uintptr_t h = make_client();

  DINGOFS_CHECK(dingofs_mount(h, MDS_ADDRS, FS_NAME, MOUNT_POINT), "dingofs_mount");

  dingofs_statfs_t st{};
  DINGOFS_CHECK(dingofs_statfs(h, &st), "dingofs_statfs");

  std::cout << "total_bytes:  " << st.total_bytes << "\n"
            << "used_bytes:   " << st.used_bytes << "\n"
            << "total_inodes: " << st.total_inodes << "\n"
            << "used_inodes:  " << st.used_inodes << "\n";

  DINGOFS_CHECK(dingofs_umount(h), "dingofs_umount");
  dingofs_delete(h);
  return 0;
}
