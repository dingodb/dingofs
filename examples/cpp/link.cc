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

// link.cc — create a hard link, stat both names, then unlink both via the C
// API.

#include <fcntl.h>
#include <sys/stat.h>

#include <iostream>

#include "common.h"

int main() {
  uintptr_t h = make_client();

  DINGOFS_CHECK(dingofs_mount(h, MDS_ADDRS, FS_NAME, MOUNT_POINT), "dingofs_mount");

  // Setup: create directory + original file
  DINGOFS_CHECK(dingofs_mkdir(h, "/demo_dir", 0755), "dingofs_mkdir");

  int fd = dingofs_open(h, "/demo_dir/original.txt",
                        O_WRONLY | O_CREAT | O_TRUNC, 0644);
  if (fd < 0) {
    std::cerr << "[FAIL] dingofs_open: " << strerror(-fd) << "\n";
    dingofs_umount(h);
    dingofs_delete(h);
    return 1;
  }
  DINGOFS_CHECK(dingofs_close(h, fd), "dingofs_close");

  // Create hard link
  DINGOFS_CHECK(
      dingofs_link(h, "/demo_dir/original.txt", "/demo_dir/hardlink.txt"),
      "dingofs_link");

  // Stat original — nlink should be 2
  struct stat st {};
  DINGOFS_CHECK(dingofs_stat(h, "/demo_dir/original.txt", &st),
                "dingofs_stat(original)");
  std::cout << "original nlink: " << st.st_nlink << "  ino: " << st.st_ino
            << "\n";

  // Stat hard link — same inode, same nlink
  struct stat lst {};
  DINGOFS_CHECK(dingofs_stat(h, "/demo_dir/hardlink.txt", &lst),
                "dingofs_stat(hardlink)");
  std::cout << "hardlink nlink: " << lst.st_nlink << "  ino: " << lst.st_ino
            << "\n";

  // Unlink hard link then original
  DINGOFS_CHECK(dingofs_unlink(h, "/demo_dir/hardlink.txt"),
                "dingofs_unlink(hardlink)");
  DINGOFS_CHECK(dingofs_unlink(h, "/demo_dir/original.txt"),
                "dingofs_unlink(original)");
  DINGOFS_CHECK(dingofs_rmdir(h, "/demo_dir"), "dingofs_rmdir");

  DINGOFS_CHECK(dingofs_umount(h), "dingofs_umount");
  dingofs_delete(h);
  return 0;
}
