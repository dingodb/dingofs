// Copyright (c) 2023 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "mdsv2/background/heartbeat.h"

#include <fmt/format.h>

#include <string>
#include <vector>

#include "fmt/core.h"
#include "mdsv2/common/codec.h"
#include "mdsv2/common/helper.h"
#include "mdsv2/common/logging.h"
#include "mdsv2/common/status.h"
#include "mdsv2/mds/mds_meta.h"
#include "mdsv2/server.h"
#include "mdsv2/storage/storage.h"

namespace dingofs {
namespace mdsv2 {

DECLARE_int32(fs_scan_batch_size);

bool Heartbeat::Init() {
  worker_ = Worker::New();
  return worker_->Init();
}

bool Heartbeat::Destroy() {
  if (worker_) {
    worker_->Destroy();
  }

  return true;
}

void Heartbeat::Run() {
  bool running = false;
  if (!is_running_.compare_exchange_strong(running, true)) {
    DINGO_LOG(INFO) << "[heartbeat] heartbeat already running......";
    return;
  }
  DEFER(is_running_.store(false));

  SendHeartbeat();
}

void Heartbeat::SendHeartbeat() {
  auto& self_mds_meta = Server::GetInstance().GetMDSMeta();

  auto mds = self_mds_meta.ToProto();
  SendHeartbeat(mds);
}

Status Heartbeat::SendHeartbeat(pb::mdsv2::MDS& mds) {
  mds.set_last_online_time_ms(Helper::TimestampMs());

  DINGO_LOG(DEBUG) << fmt::format("[heartbeat] mds {}.", mds.ShortDebugString());

  KVStorage::WriteOption option;
  auto status = kv_storage_->Put(option, MetaCodec::EncodeHeartbeatKey(mds.id()), MetaCodec::EncodeHeartbeatValue(mds));
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[heartbeat] send fail, mds({}) error({}).", mds.ShortDebugString(),
                                    status.error_str());
  }

  return status;
}

Status Heartbeat::SendHeartbeat(pb::mdsv2::Client& client) {
  client.set_last_online_time_ms(Helper::TimestampMs());

  DINGO_LOG(DEBUG) << fmt::format("[heartbeat] client {}.", client.ShortDebugString());

  KVStorage::WriteOption option;
  auto status =
      kv_storage_->Put(option, MetaCodec::EncodeHeartbeatKey(client.id()), MetaCodec::EncodeHeartbeatValue(client));
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[heartbeat] send fail, client({}) error({}).", client.ShortDebugString(),
                                    status.error_str());
  }

  return status;
}

Status Heartbeat::GetMDSList(std::vector<pb::mdsv2::MDS>& mdses) {
  Range range;
  MetaCodec::GetHeartbeatMdsRange(range.start_key, range.end_key);

  auto txn = kv_storage_->NewTxn();
  Status status;
  std::vector<KeyValue> kvs;
  do {
    kvs.clear();
    status = txn->Scan(range, FLAGS_fs_scan_batch_size, kvs);
    if (!status.ok()) {
      break;
    }

    for (auto& kv : kvs) {
      if (!MetaCodec::IsMdsHeartbeatKey(kv.key)) continue;

      pb::mdsv2::MDS mds;
      MetaCodec::DecodeHeartbeatValue(kv.value, mds);
      mdses.push_back(mds);
    }

  } while (kvs.size() >= FLAGS_fs_scan_batch_size);

  return status;
}

Status Heartbeat::GetMDSList(std::vector<MDSMeta>& mdses) {
  std::vector<pb::mdsv2::MDS> pb_mdses;
  auto status = GetMDSList(pb_mdses);
  if (!status.ok()) {
    return status;
  }

  for (auto& pb_mds : pb_mdses) {
    mdses.push_back(MDSMeta(pb_mds));
  }

  return Status::OK();
}

Status Heartbeat::GetClientList(std::vector<pb::mdsv2::Client>& clients) {
  Range range;
  MetaCodec::GetHeartbeatClientRange(range.start_key, range.end_key);

  auto txn = kv_storage_->NewTxn();
  Status status;
  std::vector<KeyValue> kvs;
  do {
    kvs.clear();
    status = txn->Scan(range, FLAGS_fs_scan_batch_size, kvs);
    if (!status.ok()) {
      break;
    }

    for (auto& kv : kvs) {
      if (!MetaCodec::IsClientHeartbeatKey(kv.key)) continue;

      pb::mdsv2::Client client;
      MetaCodec::DecodeHeartbeatValue(kv.value, client);
      clients.push_back(client);
    }

  } while (kvs.size() >= FLAGS_fs_scan_batch_size);

  return status;
}

}  // namespace mdsv2
}  // namespace dingofs
