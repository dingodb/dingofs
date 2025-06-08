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

#ifndef DINGOFS_SRC_OPTIONS_CLIENT_FUSE_H_
#define DINGOFS_SRC_OPTIONS_CLIENT_FUSE_H_

#include "options/options.h"

namespace dingofs {
namespace options {
namespace client {

class FuseConnInfoOption : public BaseOption {
  BIND_bool(want_splice_move, false, "");
  BIND_bool(want_splice_read, false, "");
  BIND_bool(want_splice_write, false, "");
  BIND_bool(want_auto_inval_data, true, "");
};

class FuseFileInfoOption : public BaseOption {
  BIND_bool(direct_io, false, "");
  BIND_bool(keep_cache, true, "");
};

class FuseOption : public BaseOption {
  BIND_suboption(conn_info, "conn_info", FuseConnInfoOption);
  BIND_suboption(file_info, "file_info", FuseFileInfoOption);
};

}  // namespace client
}  // namespace options
}  // namespace dingofs

#endif  // DINGOFS_SRC_OPTIONS_CLIENT_FUSE_H_
