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

/* options_demo.c — demonstrate conf_set/get/list/print and conf_load before
 * mounting, then mount and do a simple statfs via the C API.
 *
 * Configuration file format (gflags flags-file):
 *   Lines starting with '#' are comments.
 *   Each option is written as:  --flag_name=value
 *
 * Example /tmp/dingofs-demo.conf:
 *   # DingoFS tunable options
 *   --fuseClient.attrTimeOut=3600
 *   --fuseClient.entryTimeOut=3600
 *
 * Instance-specific keys (mds.listen.addr, log.dir, log.level, init.glog)
 * are NOT placed in the file — use dingofs_conf_set() for those.
 */

#include <stdio.h>

#include "common.h"

/* Write a minimal demo config file and return its path. */
static const char *write_demo_conf(void) {
  static const char path[] = "/tmp/dingofs-options-demo.conf";
  FILE *f = fopen(path, "w");
  if (!f) return NULL;
  (void)fprintf(f,
                "# DingoFS demo configuration file\n"
                "# Tunable gflags options — one per line.\n"
                "--fuseClient.attrTimeOut=3600\n"
                "--fuseClient.entryTimeOut=3600\n");
  (void)fclose(f);
  return path;
}

int main(void) {
  uintptr_t h = dingofs_new();

  /* --- Print all tunable options to stdout -------------------------------- */
  (void)printf("=== All tunable options (dingofs_conf_print) ===\n");
  dingofs_conf_print(h);
  (void)printf("\n");

  /* --- Enumerate options programmatically --------------------------------- */
  dingofs_option_t *opts = NULL;
  int count = 0;
  DINGOFS_CHECK(dingofs_conf_list(h, &opts, &count), "conf_list");
  (void)printf("=== dingofs_conf_list: %d options ===\n", count);
  for (int i = 0; i < count; ++i) {
    (void)printf("  %-40s (%s, default=%s)\n    %s\n", opts[i].name,
                 opts[i].type, opts[i].default_value, opts[i].description);
  }
  dingofs_conf_list_free(opts, count);
  (void)printf("\n");

  /* --- Load tunable options from a config file ---------------------------- */
  /* dingofs_conf_load() stores the file path; it is applied at mount time.  */
  /* dingofs_conf_set() calls made *after* this override file values.         */
  const char *conf_path = write_demo_conf();
  if (conf_path) {
    DINGOFS_CHECK(dingofs_conf_load(h, conf_path), "conf_load");
    (void)printf("Loaded config from: %s\n\n", conf_path);
  }

  /* --- Set instance-specific options (must use conf_set, not conf file) --- */
  DINGOFS_CHECK(
      dingofs_conf_set(h, "log.dir", "/tmp/dingofs-options-demo-log"),
      "conf_set log.dir");
  DINGOFS_CHECK(dingofs_conf_set(h, "log.level", "WARNING"),
                "conf_set log.level");
  /* --- Read them back ----------------------------------------------------- */
  char buf[512] = {0};

  DINGOFS_CHECK(dingofs_conf_get(h, "log.dir", buf, sizeof(buf)),
                "conf_get log.dir");
  (void)printf("log.dir   = %s\n", buf);

  DINGOFS_CHECK(dingofs_conf_get(h, "log.level", buf, sizeof(buf)),
                "conf_get log.level");
  (void)printf("log.level = %s\n\n", buf);

  /* --- Mount and do a simple statfs --------------------------------------- */
  DINGOFS_CHECK(dingofs_mount(h, MDS_ADDRS, FS_NAME, MOUNT_POINT), "dingofs_mount");

  dingofs_statfs_t st;
  DINGOFS_CHECK(dingofs_statfs(h, &st), "dingofs_statfs");

  (void)printf("total_bytes:   %lld\n", (long long)st.total_bytes);
  (void)printf("used_bytes:    %lld\n", (long long)st.used_bytes);
  (void)printf("total_inodes:  %lld\n", (long long)st.total_inodes);
  (void)printf("used_inodes:   %lld\n", (long long)st.used_inodes);

  DINGOFS_CHECK(dingofs_umount(h), "dingofs_umount");
  dingofs_delete(h);
  return 0;
}
