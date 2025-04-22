/*
 * Copyright (c) 2025 dingodb.com, Inc. All Rights Reserved
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
 * Project: DingoFS
 * Created Date: 2025-05-07
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_OPTIONS_CLIENT_VFS_H_
#define DINGOFS_SRC_OPTIONS_CLIENT_VFS_H_

#include "options/client/data.h"
#include "options/client/meta.h"
#include "options/options.h"

namespace dingofs {
namespace options {
namespace client {

class KernelCacheOption : public BaseOption {
  BIND_double(attr_timeout_s, 30, "");
  BIND_double(dir_attr_timeout_s, 30, "");
  BIND_double(entry_timeout_s, 30, "");
  BIND_double(dir_entry_timeout_s, 30, "");
};

class LookupCacheOption : public BaseOption {
  BIND_int32(negative_timeout_s, 0, "");
  BIND_int32(min_uses, 1, "");
  BIND_int32(lru_size, 100000, "");
};

class DirCacheOption : public BaseOption {
  BIND_int32(lru_size, 5000000, "");
};

class AttrWatcherOption : public BaseOption {
  BIND_int32(lru_size, 5000000, "");
};

class RpcOption : public BaseOption {
  BIND_int32(list_dentry_limit, 65536, "");
};

class DeferSyncOption : public BaseOption {
  BIND_int32(delay_s, 3, "");
  BIND_int32(defer_dir_mtime, false, "");
};

class QuotaOption : public BaseOption {
  BIND_uint32(flush_quota_interval_s, 0, "");
  BIND_uint32(load_quota_interval_s, 0, "");
};

class LeaseOption : public BaseOption {
  BIND_uint32(lease_times_us, 20000000, "");
  BIND_uint32(refresh_times_per_lease, 5, "");
};

class ThrottleOption : public BaseOption {
  BIND_uint32(avg_write_bytes, 0, "");
  BIND_uint32(burst_write_bytes, 0, "");
  BIND_uint32(burst_write_bytes_s, 180, "");
  BIND_uint32(avg_write_iops, 0, "");
  BIND_uint32(burst_write_iops, 0, "");
  BIND_uint32(burst_write_iops_s, 180, "");
  BIND_uint32(avg_read_bytes, 0, "");
  BIND_uint32(burst_read_bytes, 0, "");
  BIND_uint32(burst_read_bytes_s, 180, "");
  BIND_uint32(avg_read_iops, 0, "");
  BIND_uint32(burst_read_iops, 0, "");
  BIND_uint32(burst_read_iops_s, 180, "");
};

class VFSOption : public BaseOption {
  BIND_bool(cto, true, "");
  BIND_string(nocto_suffix, " ", "");
  BIND_int32(max_name_length, 255, "");
  BIND_bool(disable_xattr, true, "");

  BIND_suboption(kernel_cache_option, "kernel", KernelCacheOption);
  BIND_suboption(lookup_cache_option, "lookup_cache", LookupCacheOption);
  BIND_suboption(dir_cache_option, "dir_cache", DirCacheOption);
  BIND_suboption(attr_watcher_option, "attr_watcher", AttrWatcherOption);
  BIND_suboption(rpc_option, "rpc_option", RpcOption);
  BIND_suboption(defer_sync_option, "defer_sync", DeferSyncOption);
  BIND_suboption(quota_option, "quota", QuotaOption);
  BIND_suboption(lease_option, "lease", LeaseOption);

  BIND_suboption(meta_option, "meta", MetaOption);
  BIND_suboption(data_option, "data", DataOption);
};

}  // namespace client
}  // namespace options
}  // namespace dingofs

#endif  // DINGOFS_SRC_OPTIONS_CLIENT_VFS_H
