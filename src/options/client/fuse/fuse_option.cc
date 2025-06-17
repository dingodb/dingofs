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

#include "options/client/fuse/fuse_option.h"

#include "options/client/fuse/fuse_dynamic_option.h"

namespace dingofs {
namespace client {

void InitFuseOption(utils::Configuration* c, FuseOption* option) {
  {  // fuse conn info
    auto* o = &option->conn_info;
    c->GetValueFatalIfFail("fuse.conn_info.want_splice_move",
                           &o->want_splice_move);
    c->GetValueFatalIfFail("fuse.conn_info.want_splice_read",
                           &o->want_splice_read);
    c->GetValueFatalIfFail("fuse.conn_info.want_splice_write",
                           &o->want_splice_write);
    c->GetValueFatalIfFail("fuse.conn_info.want_auto_inval_data",
                           &o->want_auto_inval_data);
  }

  {  // fuse file info
    c->GetValueFatalIfFail("fuse.file_info.direct_io",
                           &FLAGS_fuse_file_info_direct_io);
    c->GetValueFatalIfFail("fuse.file_info.keep_cache",
                           &FLAGS_fuse_file_info_keep_cache);
  }
}

}  // namespace client
}  // namespace dingofs
