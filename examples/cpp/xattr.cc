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

// xattr.cc — setxattr, getxattr, listxattr, removexattr via the C API.

#include <fcntl.h>

#include <iostream>
#include <string>
#include <vector>

#include "common.h"

int main() {
  uintptr_t h = make_client();

  DINGOFS_CHECK(dingofs_mount(h, MDS_ADDRS, FS_NAME, MOUNT_POINT), "dingofs_mount");

  // Setup: create directory + file
  DINGOFS_CHECK(dingofs_mkdir(h, "/demo_dir", 0755), "dingofs_mkdir");

  int fd = dingofs_open(h, "/demo_dir/hello.txt", O_WRONLY | O_CREAT | O_TRUNC,
                        0644);
  if (fd < 0) {
    std::cerr << "[FAIL] dingofs_open: " << strerror(-fd) << "\n";
    dingofs_umount(h);
    dingofs_delete(h);
    return 1;
  }
  DINGOFS_CHECK(dingofs_close(h, fd), "dingofs_close");

  const char* path = "/demo_dir/hello.txt";

  // SetXattr
  const char* author_val = "dingodb";
  DINGOFS_CHECK(dingofs_setxattr(h, path, "user.author", author_val,
                                 strlen(author_val), 0),
                "dingofs_setxattr(user.author)");

  const char* version_val = "1.0";
  DINGOFS_CHECK(dingofs_setxattr(h, path, "user.version", version_val,
                                 strlen(version_val), 0),
                "dingofs_setxattr(user.version)");

  // GetXattr
  char vbuf[256] = {};
  ssize_t vsz = dingofs_getxattr(h, path, "user.author", vbuf, sizeof(vbuf));
  if (vsz < 0) {
    std::cerr << "[FAIL] dingofs_getxattr: " << strerror(static_cast<int>(-vsz))
              << "\n";
    dingofs_umount(h);
    dingofs_delete(h);
    return 1;
  }
  std::string got_val(vbuf, static_cast<size_t>(vsz));
  std::cout << "[ OK ] dingofs_getxattr  user.author=" << got_val << "\n";

  // ListXattr — query size then read
  ssize_t lsz = dingofs_listxattr(h, path, nullptr, 0);
  if (lsz < 0) {
    std::cerr << "[FAIL] dingofs_listxattr(size query): "
              << strerror(static_cast<int>(-lsz)) << "\n";
    dingofs_umount(h);
    dingofs_delete(h);
    return 1;
  }
  std::vector<char> lbuf(static_cast<size_t>(lsz));
  lsz = dingofs_listxattr(h, path, lbuf.data(), lbuf.size());
  if (lsz < 0) {
    std::cerr << "[FAIL] dingofs_listxattr: "
              << strerror(static_cast<int>(-lsz)) << "\n";
    dingofs_umount(h);
    dingofs_delete(h);
    return 1;
  }
  std::cout << "[ OK ] dingofs_listxattr  xattrs:\n";
  for (const char* p = lbuf.data(); p < lbuf.data() + lsz; p += strlen(p) + 1) {
    std::cout << "  " << p << "\n";
  }

  // RemoveXattr
  DINGOFS_CHECK(dingofs_removexattr(h, path, "user.author"),
                "dingofs_removexattr(user.author)");
  DINGOFS_CHECK(dingofs_removexattr(h, path, "user.version"),
                "dingofs_removexattr(user.version)");

  // Cleanup
  DINGOFS_CHECK(dingofs_unlink(h, "/demo_dir/hello.txt"), "dingofs_unlink");
  DINGOFS_CHECK(dingofs_rmdir(h, "/demo_dir"), "dingofs_rmdir");

  DINGOFS_CHECK(dingofs_umount(h), "dingofs_umount");
  dingofs_delete(h);
  return 0;
}
