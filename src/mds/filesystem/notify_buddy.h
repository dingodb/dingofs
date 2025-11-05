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

#ifndef DINGOFS_MDS_FILESYSTEM_NOTIFY_BUDDY_H_
#define DINGOFS_MDS_FILESYSTEM_NOTIFY_BUDDY_H_

#include <sys/types.h>

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "bthread/types.h"
#include "butil/containers/mpsc_queue.h"
#include "butil/endpoint.h"
#include "mds/common/type.h"
#include "mds/mds/mds_meta.h"

namespace dingofs {
namespace mds {
namespace notify {

enum class Type : int8_t {
  kRefreshFsInfo = 0,
  kRefreshInode = 1,
  kCleanPartitionCache = 2,
  kSetDirQuota = 3,
  kDeleteDirQuota = 4,
};

struct Message {
  Message(Type type, uint64_t mds_id, uint32_t fs_id) : type(type), mds_id(mds_id), fs_id(fs_id) {}
  Message(Type type, uint64_t mds_id, uint32_t fs_id, uint64_t version)
      : type(type), mds_id(mds_id), fs_id(fs_id), version(version) {}
  virtual ~Message() = default;

  Type type;
  uint64_t mds_id{0};
  uint32_t fs_id{0};
  uint64_t version{0};
};

using MessageSPtr = std::shared_ptr<Message>;

struct RefreshFsInfoMessage : public Message {
  RefreshFsInfoMessage(uint64_t mds_id, uint32_t fs_id, const std::string& fs_name)
      : Message{Type::kRefreshFsInfo, mds_id, fs_id}, fs_name(fs_name) {}

  static MessageSPtr Create(uint64_t mds_id, uint32_t fs_id, const std::string& fs_name) {
    return std::make_shared<RefreshFsInfoMessage>(mds_id, fs_id, fs_name);
  }

  std::string fs_name;
};

struct RefreshInodeMessage : public Message {
  RefreshInodeMessage(uint64_t mds_id, uint32_t fs_id, AttrEntry&& attr)
      : Message{Type::kRefreshInode, mds_id, fs_id}, attr(std::move(attr)) {}

  static MessageSPtr Create(uint64_t mds_id, uint32_t fs_id, AttrEntry&& attr) {
    return std::make_shared<RefreshInodeMessage>(mds_id, fs_id, std::move(attr));
  }

  AttrEntry attr;
};

struct CleanPartitionCacheMessage : public Message {
  CleanPartitionCacheMessage(uint64_t mds_id, uint32_t fs_id, Ino ino, uint64_t version)
      : Message{Type::kCleanPartitionCache, mds_id, fs_id, version}, ino(ino) {}

  static MessageSPtr Create(uint64_t mds_id, uint32_t fs_id, Ino ino, uint64_t version) {
    return std::make_shared<CleanPartitionCacheMessage>(mds_id, fs_id, ino, version);
  }

  Ino ino{0};
};

struct SetDirQuotaMessage : public Message {
  SetDirQuotaMessage(uint64_t mds_id, uint32_t fs_id, Ino ino, const QuotaEntry& quota)
      : Message{Type::kSetDirQuota, mds_id, fs_id}, ino(ino), quota(quota) {}

  static MessageSPtr Create(uint64_t mds_id, uint32_t fs_id, Ino ino, const QuotaEntry& quota) {
    return std::make_shared<SetDirQuotaMessage>(mds_id, fs_id, ino, quota);
  }

  Ino ino{0};
  QuotaEntry quota;
};

struct DeleteDirQuotaMessage : public Message {
  DeleteDirQuotaMessage(uint64_t mds_id, uint32_t fs_id, Ino ino, const std::string& uuid)
      : Message{Type::kDeleteDirQuota, mds_id, fs_id}, ino(ino), uuid(uuid) {}

  static MessageSPtr Create(uint64_t mds_id, uint32_t fs_id, Ino ino, const std::string& uuid) {
    return std::make_shared<DeleteDirQuotaMessage>(mds_id, fs_id, ino, uuid);
  }

  Ino ino{0};
  std::string uuid;
};

class NotifyBuddy;
using NotifyBuddySPtr = std::shared_ptr<NotifyBuddy>;

class NotifyBuddy {
 public:
  NotifyBuddy(MDSMetaMapSPtr mds_meta_map, uint64_t self_mds_id);
  ~NotifyBuddy();

  static NotifyBuddySPtr New(MDSMetaMapSPtr mds_meta_map, uint64_t self_mds_id) {
    return std::make_shared<NotifyBuddy>(mds_meta_map, self_mds_id);
  }

  bool Init();
  bool Destroy();

  bool AsyncNotify(MessageSPtr message);

 private:
  using BatchMessage = std::vector<MessageSPtr>;

  // mds_id -> messages
  static std::map<uint64_t, BatchMessage> GroupingByMdsID(const std::vector<MessageSPtr>& messages);
  void DispatchMessage();
  void LaunchSendMessage(uint64_t mds_id, const BatchMessage& batch_message);
  void SendMessage(uint64_t mds_id, BatchMessage& batch_message);

  bool GenEndpoint(uint64_t mds_id, butil::EndPoint& endpoint);

  bthread_t tid_{0};
  bthread_mutex_t mutex_;
  bthread_cond_t cond_;

  std::atomic<bool> is_stop_{false};

  std::atomic<uint64_t> id_generator_{0};

  butil::MPSCQueue<MessageSPtr> queue_;

  uint64_t self_mds_id_;
  MDSMetaMapSPtr mds_meta_map_;
};

}  // namespace notify
}  // namespace mds
}  // namespace dingofs

#endif  // DINGOFS_MDS_FILESYSTEM_NOTIFY_BUDDY_H_