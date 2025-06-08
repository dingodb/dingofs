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

#ifndef DINGOFS_SRC_OPTIONS_CLIENT_META_H_
#define DINGOFS_SRC_OPTIONS_CLIENT_META_H_

#include "options/options.h"

namespace dingofs {
namespace options {
namespace client {

namespace v1 {

class MDSOption {};

class MetaOption : public BaseOption {
  BIND_suboption(mds_option, "mds", MDSOption);
};

};  // namespace v1

namespace v2 {

class MDSOption : public BaseOption {
  BIND_uint32(rpc_timeout_ms, 2000, "");
  BIND_uint32(rpc_retry_times, 3, "");
  BIND_uint32(client_send_request_retry_times, 3, "");
};

class MetaOption : public BaseOption {
  BIND_suboption(mds_option, "mds", MDSOption);
};

};  // namespace v2

class MetaOption : public BaseOption {
  BIND_suboption(v1, "v1", v1::MetaOption);
  BIND_suboption(v2, "v2", v2::MetaOption);
};

}  // namespace client
}  // namespace options
}  // namespace dingofs

#endif  // DINGOFS_SRC_OPTIONS_CLIENT_META_H_
