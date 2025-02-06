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
 * Created Date: 2025-02-10
 * Author: Jingli Chen (Wine93)
 */

#include <butil/endpoint.h>

#include "client/cachegroup/client/cache_group_node.h"

namespace dingofs {
namespace cache {
namespace cachegroup {

using ::dingofs::pb::client::blockcache::BlockCacheErrCode;

RemoteNodeImpl::CacheGroupNodeImpl(const CacheGroupMember& member)
    : member_(member) {}

bool CacheGroupNodeImpl::Init() {
  std::string listen_ip = member_.ip();
  uint32_t listen_port = member_.port();

  ::butil::EndPoint ep;
  int rc = ::butil::str2endpoint(listen_ip.c_str(), listen_port, &ep);
  if (rc != 0) {
    LOG(ERROR) << "str2endpoint(" << listen_ip << "," << listen_port
               << ") failed, rc=" << rc;
    return false;
  }

  rc = channel_->Init(ep, nullptr);
  if (rc != 0) {
    LOG(INFO) << "Init channel for " << listen_ip << ":" << listen_port
              << " failed, rc=" << rc;
    return false;
  }

  LOG(INFO) << "Create channel for " << listen_ip << ":" << listen_port
            << " success.";
  return true;
}

namespace {

::dingofs::pb::client::blockcache::BlockKey PbBlockKey(const BlockKey& key) {
  ::dingofs::pb::client::blockcache::BlockKey pb_key;
  pb_key.set_fs_id(key.fs_id);
  pb_key.set_ino(key.ino);
  pb_key.set_id(key.id);
  pb_key.set_index(key.index);
  pb_key.set_version(key.version);
  return pb_key;
}

};  // namespace

Errno CacheGroupNodeImpl::Range(const BlockKey& block_key, off_t offset,
                                size_t length, ::butil::IOBuf* buffer) {
  ::brpc::Controller cntl;
  ::dingofs::pb::client::blockcache::RangeRequest request;
  ::dingofs::pb::client::blockcache::RangeResponse response;

  *request.mutable_block_key() = PbBlockKey(block_key);
  request.set_offset(offset);
  request.set_length(length);

  cntl.set_timeout_ms(3000);
  ::dingofs::pb::client::blockcache::BlockCacheService_Stub stub(
      channel_.get());
  stub.Range(&cntl, &request, &response, nullptr);

  if (!cntl.Failed()) {
    auto status = response.status();
    if (status == BlockCacheErrCode::BlockCacheOk) {
      *buffer = cntl.response_attachment();
      return Errno::kOk;
    }
  }
  return Errno::kFail;
}

CacheGroupMember& CacheGroupNodeImpl::GetMember() { return member_; }

}  // namespace cachegroup
}  // namespace cache
}  // namespace dingofs
