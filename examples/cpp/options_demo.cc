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

// options_demo.cc — demonstrate conf_load/conf_set/get/list/print before
// mounting, then mount and do a simple statfs via the C API.
//
// Configuration file format (gflags flags-file):
//   Lines starting with '#' are comments.
//   Each option is written as:  --flag_name=value
//
// Example /tmp/dingofs-demo.conf:
//   # DingoFS tunable options
//   --fuseClient.attrTimeOut=3600
//   --fuseClient.entryTimeOut=3600
//
// Instance-specific keys (mds.listen.addr, log.dir, log.level, init.glog)
// are NOT placed in the file — use dingofs_conf_set() for those.

#include <fstream>
#include <iostream>
#include <string>

#include "common.h"

// Write a minimal demo config file; return its path or empty string on error.
static std::string write_demo_conf() {
  const std::string path = "/tmp/dingofs-options-demo.conf";
  std::ofstream f(path);
  if (!f) return {};
  f << "# DingoFS demo configuration file\n"
    << "# Tunable gflags options — one per line.\n"
    << "--fuseClient.attrTimeOut=3600\n"
    << "--fuseClient.entryTimeOut=3600\n";
  return path;
}

int main() {
  uintptr_t h = dingofs_new();

  // --- Print all tunable options to stdout ----------------------------------
  // Useful for discovering available keys without reading the source.
  std::cout << "=== All tunable options (dingofs_conf_print) ===\n";
  dingofs_conf_print(h);
  std::cout << "\n";

  // --- Enumerate options programmatically -----------------------------------
  dingofs_option_t* opts = nullptr;
  int count = 0;
  DINGOFS_CHECK(dingofs_conf_list(h, &opts, &count), "conf_list");
  std::cout << "=== dingofs_conf_list: " << count << " options ===\n";
  for (int i = 0; i < count; ++i) {
    std::cout << "  " << opts[i].name << " (" << opts[i].type
              << ", default=" << opts[i].default_value << ")\n"
              << "    " << opts[i].description << "\n";
  }
  dingofs_conf_list_free(opts, count);
  std::cout << "\n";

  // --- Load tunable options from a config file ------------------------------
  // dingofs_conf_load() stores the file path; it is applied at mount time.
  // dingofs_conf_set() calls made *after* this override file values.
  std::string conf_path = write_demo_conf();
  if (!conf_path.empty()) {
    DINGOFS_CHECK(dingofs_conf_load(h, conf_path.c_str()), "conf_load");
    std::cout << "Loaded config from: " << conf_path << "\n\n";
  }

  // --- Set instance-specific options (must use conf_set, not conf file) -----
  DINGOFS_CHECK(dingofs_conf_set(h, "log.dir", "/tmp/dingofs-options-demo-log"),
                "conf_set log.dir");
  DINGOFS_CHECK(dingofs_conf_set(h, "log.level", "WARNING"),
                "conf_set log.level");

  // --- Read them back -------------------------------------------------------
  char buf[512] = {};

  DINGOFS_CHECK(dingofs_conf_get(h, "log.dir", buf, sizeof(buf)),
                "conf_get log.dir");
  std::cout << "log.dir   = " << buf << "\n";

  DINGOFS_CHECK(dingofs_conf_get(h, "log.level", buf, sizeof(buf)),
                "conf_get log.level");
  std::cout << "log.level = " << buf << "\n\n";

  // --- Mount and do a simple statfs -----------------------------------------
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
