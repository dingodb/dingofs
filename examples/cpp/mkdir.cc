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

// mkdir.cc — mkdir, opendir, readdir, closedir, rmdir via the C API.

#include <iostream>

#include "common.h"

int main() {
  uintptr_t h = make_client();

  DINGOFS_CHECK(dingofs_mount(h, MDS_ADDRS, FS_NAME, MOUNT_POINT), "dingofs_mount");

  // MkDir
  DINGOFS_CHECK(dingofs_mkdir(h, "/demo_dir", 0755), "dingofs_mkdir");

  // OpenDir
  uint64_t dh = 0;
  DINGOFS_CHECK(dingofs_opendir(h, "/demo_dir", &dh), "dingofs_opendir");

  // ReadDir — read all entries in batches of 64
  dingofs_dirent_t entries[64];
  int n = 0;
  while ((n = dingofs_readdir(h, dh, entries, 64)) > 0) {
    for (int i = 0; i < n; ++i) {
      std::cout << "  entry: " << entries[i].d_name
                << "  ino=" << entries[i].d_ino
                << "  type=" << static_cast<int>(entries[i].d_type) << "\n";
    }
  }
  if (n < 0) {
    std::cerr << "[FAIL] dingofs_readdir: " << strerror(-n) << "\n";
    dingofs_closedir(h, dh);
    dingofs_umount(h);
    dingofs_delete(h);
    return 1;
  }

  // CloseDir
  DINGOFS_CHECK(dingofs_closedir(h, dh), "dingofs_closedir");

  // RmDir
  DINGOFS_CHECK(dingofs_rmdir(h, "/demo_dir"), "dingofs_rmdir");

  DINGOFS_CHECK(dingofs_umount(h), "dingofs_umount");
  dingofs_delete(h);
  return 0;
}
