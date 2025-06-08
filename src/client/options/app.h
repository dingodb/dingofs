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

#ifndef DINGOFS_SRC_OPTIONS_CLIENT_APP_H_
#define DINGOFS_SRC_OPTIONS_CLIENT_APP_H_

#include "options/cache/block_cache.h"
#include "options/client/data.h"
#include "options/client/fuse.h"
#include "options/client/global.h"
#include "options/client/meta.h"
#include "options/client/rpc.h"
#include "options/client/s3.h"
#include "options/client/vfs.h"
#include "options/options.h"

// options level layout:
//                                 ┌─> meta -> rpc
//   app -> global -> fuse -> vfs -|
//                                 └─> data -> { blockcache, s3 }
namespace dingofs {
namespace options {
namespace client {

class AppOption : public BaseOption {
  BIND_suboption(global_option, "global", GlobalOption);
  BIND_suboption(fuse_option, "fuse", FuseOption);
  BIND_suboption(vfs_option, "vfs", VFSOption);
  BIND_suboption(meta_option, "meta", MetaOption);
  BIND_suboption(data_option, "data", DataOption);
  BIND_suboption(rpc_option, "rpc", RPCOption);
  BIND_suboption(block_cache_option, "block_cache", cache::BlockCacheOption);
  BIND_suboption(s3_option, "s3", S3Option);
};

DECLARE_OPTION(client, AppOption);

}  // namespace client
}  // namespace options
}  // namespace dingofs

#endif  // DINGOFS_SRC_OPTIONS_CLIENT_APP_H_
