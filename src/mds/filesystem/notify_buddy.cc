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

#include "mds/filesystem/notify_buddy.h"

#include <cstdint>
#include <memory>

#include "bthread/bthread.h"
#include "mds/common/logging.h"
#include "mds/service/service_access.h"

namespace dingofs {
namespace mds {
namespace notify {

DEFINE_uint32(mds_notify_message_batch_size, 64, "notify message batch size.");
DEFINE_validator(mds_notify_message_batch_size, brpc::PassValidate);

DEFINE_uint32(mds_wait_message_delay_us, 100, "wait message delay us.");
DEFINE_validator(mds_wait_message_delay_us, brpc::PassValidate);

NotifyBuddy::NotifyBuddy(MDSMetaMapSPtr mds_meta_map, uint64_t self_mds_id)
    : mds_meta_map_(mds_meta_map), self_mds_id_(self_mds_id) {
  bthread_mutex_init(&mutex_, nullptr);
  bthread_cond_init(&cond_, nullptr);
}

NotifyBuddy::~NotifyBuddy() {
  bthread_cond_destroy(&cond_);
  bthread_mutex_destroy(&mutex_);
}

bool NotifyBuddy::Init() {
  struct Param {
    NotifyBuddy* self{nullptr};
  };

  Param* param = new Param({this});

  const bthread_attr_t attr = BTHREAD_ATTR_NORMAL;
  if (bthread_start_background(
          &tid_, &attr,
          [](void* arg) -> void* {
            Param* param = reinterpret_cast<Param*>(arg);

            param->self->DispatchMessage();

            delete param;
            return nullptr;
          },
          param) != 0) {
    tid_ = 0;
    delete param;
    LOG(FATAL) << "[notify] start background thread fail.";
    return false;
  }

  return true;
}

bool NotifyBuddy::Destroy() {
  is_stop_.store(true);

  if (tid_ > 0) {
    bthread_cond_signal(&cond_);

    if (bthread_stop(tid_) != 0) {
      LOG(ERROR) << fmt::format("[notify] bthread_stop fail.");
    }

    if (bthread_join(tid_, nullptr) != 0) {
      LOG(ERROR) << fmt::format("[notify] bthread_join fail.");
    }
  }

  return true;
}

bool NotifyBuddy::AsyncNotify(MessageSPtr message) {
  if (is_stop_.load(std::memory_order_relaxed)) {
    return false;
  }

  if (message->mds_id == self_mds_id_) {
    // do not send message to self
    return false;
  }

  queue_.Enqueue(message);

  bthread_cond_signal(&cond_);

  return true;
}

std::map<uint64_t, NotifyBuddy::BatchMessage> NotifyBuddy::GroupingByMdsID(const std::vector<MessageSPtr>& messages) {
  std::map<uint64_t, NotifyBuddy::BatchMessage> batch_message_map;

  for (const auto& message : messages) {
    if (message == nullptr) {
      continue;
    }

    auto it = batch_message_map.find(message->mds_id);
    if (it == batch_message_map.end()) {
      batch_message_map[message->mds_id] = BatchMessage{message};

    } else {
      it->second.push_back(message);
    }
  }

  return batch_message_map;
}

void NotifyBuddy::DispatchMessage() {
  std::vector<MessageSPtr> messages;
  messages.reserve(FLAGS_mds_notify_message_batch_size);

  while (true) {
    messages.clear();

    MessageSPtr message = nullptr;
    while (!queue_.Dequeue(message) && !is_stop_.load(std::memory_order_relaxed)) {
      bthread_mutex_lock(&mutex_);
      bthread_cond_wait(&cond_, &mutex_);
      bthread_mutex_unlock(&mutex_);
    }

    if (is_stop_.load(std::memory_order_relaxed) && messages.empty()) {
      break;
    }

    if (FLAGS_mds_wait_message_delay_us > 0) {
      bthread_usleep(FLAGS_mds_wait_message_delay_us);
    }

    do {
      messages.push_back(message);
    } while (queue_.Dequeue(message));

    auto batch_message_map = GroupingByMdsID(messages);
    for (auto& [mds_id, batch_message] : batch_message_map) {
      LaunchSendMessage(mds_id, batch_message);
    }
  }
}

void NotifyBuddy::LaunchSendMessage(uint64_t mds_id, const BatchMessage& batch_message) {
  struct Params {
    NotifyBuddy* self{nullptr};
    uint64_t mds_id;
    BatchMessage batch_message;
  };

  Params* params = new Params({.self = this, .mds_id = mds_id, .batch_message = batch_message});

  bthread_t tid;
  bthread_attr_t attr = BTHREAD_ATTR_SMALL;
  if (bthread_start_background(
          &tid, &attr,
          [](void* arg) -> void* {
            Params* params = reinterpret_cast<Params*>(arg);

            params->self->SendMessage(params->mds_id, params->batch_message);

            delete params;

            return nullptr;
          },
          params) != 0) {
    delete params;
    LOG(FATAL) << "[notify] start background thread fail.";
  }
}

void NotifyBuddy::SendMessage(uint64_t mds_id, BatchMessage& batch_message) {
  pb::mds::NotifyBuddyRequest notify_message;
  notify_message.set_id(id_generator_.fetch_add(1, std::memory_order_relaxed));

  for (auto& message : batch_message) {
    auto* mut_message = notify_message.add_messages();
    mut_message->set_fs_id(message->fs_id);

    switch (message->type) {
      case Type::kRefreshFsInfo: {
        mut_message->set_type(pb::mds::NotifyBuddyRequest::TYPE_REFRESH_FS_INFO);

        auto refresh_fs_info_message = std::dynamic_pointer_cast<RefreshFsInfoMessage>(message);
        mut_message->mutable_refresh_fs_info()->set_fs_name(refresh_fs_info_message->fs_name);

        DINGO_LOG(INFO) << fmt::format("[notify.{}] refresh fs({}/{}) info.", mds_id, message->fs_id,
                                       refresh_fs_info_message->fs_name);

      } break;

      case Type::kRefreshInode: {
        mut_message->set_type(pb::mds::NotifyBuddyRequest::TYPE_REFRESH_INODE);

        auto* mut_refresh_inode = mut_message->mutable_refresh_inode();
        auto refresh_inode_message = std::dynamic_pointer_cast<RefreshInodeMessage>(message);
        mut_refresh_inode->mutable_inode()->Swap(&refresh_inode_message->attr);

        DINGO_LOG(INFO) << fmt::format("[notify.{}] refresh inode, inode({}).", mds_id,
                                       mut_refresh_inode->inode().ShortDebugString());

      } break;

      case Type::kCleanPartitionCache: {
        mut_message->set_type(pb::mds::NotifyBuddyRequest::TYPE_CLEAN_PARTITION_CACHE);

        auto clean_partition_cache_message = std::dynamic_pointer_cast<CleanPartitionCacheMessage>(message);
        mut_message->mutable_clean_partition_cache()->set_ino(clean_partition_cache_message->ino);

        DINGO_LOG(INFO) << fmt::format("[notify.{}] clean partition cache({}/{}) info.", mds_id, message->fs_id,
                                       clean_partition_cache_message->ino);

      } break;

      case Type::kSetDirQuota: {
        mut_message->set_type(pb::mds::NotifyBuddyRequest::TYPE_SET_DIR_QUOTA);

        auto set_dir_quota_message = std::dynamic_pointer_cast<SetDirQuotaMessage>(message);
        mut_message->mutable_set_dir_quota()->set_ino(set_dir_quota_message->ino);
        mut_message->mutable_set_dir_quota()->mutable_quota()->Swap(&set_dir_quota_message->quota);

        DINGO_LOG(INFO) << fmt::format("[notify.{}] set dir quota, dir({}/{}) quota({}).", mds_id, message->fs_id,
                                       set_dir_quota_message->ino, set_dir_quota_message->quota.ShortDebugString());

      } break;

      case Type::kDeleteDirQuota: {
        mut_message->set_type(pb::mds::NotifyBuddyRequest::TYPE_DELETE_DIR_QUOTA);

        auto delete_dir_quota_message = std::dynamic_pointer_cast<DeleteDirQuotaMessage>(message);
        mut_message->mutable_delete_dir_quota()->set_ino(delete_dir_quota_message->ino);
        mut_message->mutable_delete_dir_quota()->set_uuid(delete_dir_quota_message->uuid);

        DINGO_LOG(INFO) << fmt::format("[notify.{}] delete dir quota, dir({}/{}/{}).", mds_id, message->fs_id,
                                       delete_dir_quota_message->ino, delete_dir_quota_message->uuid);

      } break;

      default:
        DINGO_LOG(FATAL) << fmt::format("[notify] unknown message type: {}.", static_cast<int>(message->type));
        break;
    }
  }

  butil::EndPoint endpoint;
  if (!GenEndpoint(mds_id, endpoint)) {
    DINGO_LOG(ERROR) << fmt::format("[notify.{}] gen endpoint fail.", mds_id);
    return;
  }

  auto status = ServiceAccess::NotifyBuddy(endpoint, notify_message);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[notify.{}] send message fail, {}.", mds_id, status.error_str());
  }
}

bool NotifyBuddy::GenEndpoint(uint64_t mds_id, butil::EndPoint& endpoint) {
  MDSMeta mds_meta;
  if (!mds_meta_map_->GetMDSMeta(mds_id, mds_meta)) {
    return false;
  }

  if (butil::str2endpoint(mds_meta.Host().c_str(), mds_meta.Port(), &endpoint) != 0) {
    return false;
  }

  return true;
}

}  // namespace notify
}  // namespace mds
}  // namespace dingofs