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
#include <iterator>
#include <memory>
#include <system_error>
#include <utility>

#include "bthread/bthread.h"
#include "common/logging.h"
#include "mds/service/service_access.h"

namespace dingofs {
namespace mds {
namespace notify {

DEFINE_uint32(mds_notify_message_batch_size, 124, "notify message batch size.");
DEFINE_validator(mds_notify_message_batch_size, brpc::PassValidate);

DEFINE_uint32(mds_notify_message_max_inflight_per_mds, 1, "max in-flight notify message batches per mds.");
DEFINE_validator(mds_notify_message_max_inflight_per_mds, brpc::PositiveInteger);

NotifyBuddy::NotifyBuddy(MDSMetaMapSPtr mds_meta_map, uint64_t self_mds_id)
    : self_mds_id_(self_mds_id), mds_meta_map_(mds_meta_map) {}

NotifyBuddy::~NotifyBuddy() { Destroy(); }

bool NotifyBuddy::Init() {
  try {
    dispatcher_.thread = std::thread([this] { DispatchMessage(); });
  } catch (const std::system_error& e) {
    LOG(ERROR) << fmt::format("[notify] start background thread fail, {}.", e.what());
    return false;
  }

  return true;
}

bool NotifyBuddy::Destroy() {
  bool expected = false;
  if (!is_stop_.compare_exchange_strong(expected, true)) return true;

  {
    std::lock_guard<std::mutex> lock(dispatcher_.thread_mutex);
    dispatcher_.thread_cond.notify_all();
  }

  if (dispatcher_.thread.joinable()) dispatcher_.thread.join();

  while (sending_tasks_.load(std::memory_order_acquire) > 0) {
    bthread_usleep(1000);
  }

  return true;
}

bool NotifyBuddy::AsyncNotify(MessageSPtr message) {
  if (message == nullptr || is_stop_.load(std::memory_order_acquire)) {
    return false;
  }

  if (message->mds_id == self_mds_id_) {
    // do not send message to self
    return false;
  }

  {
    // enqueue and notify under thread_mutex so the dispatcher cannot miss a
    // wakeup, and re-check is_stop_ so no message slips in after the shutdown
    // drain in DispatchMessage.
    std::lock_guard<std::mutex> lock(dispatcher_.thread_mutex);
    if (is_stop_.load(std::memory_order_acquire)) return false;

    dispatcher_.messages.Enqueue(std::move(message));
    dispatcher_.thread_cond.notify_one();
  }

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

  MessageSPtr message = nullptr;
  while (!is_stop_.load(std::memory_order_acquire)) {
    messages.clear();

    message = nullptr;
    {
      std::unique_lock<std::mutex> lock(dispatcher_.thread_mutex);
      while (!dispatcher_.messages.Dequeue(message) && !is_stop_.load(std::memory_order_acquire)) {
        dispatcher_.thread_cond.wait(lock);
      }
    }

    if (is_stop_.load(std::memory_order_acquire)) break;

    messages.push_back(std::move(message));

    while (messages.size() < FLAGS_mds_notify_message_batch_size && dispatcher_.messages.Dequeue(message)) {
      messages.push_back(std::move(message));
    }

    auto batch_message_map = GroupingByMdsID(messages);
    for (auto& [mds_id, batch_message] : batch_message_map) {
      LaunchOrParkMessage(mds_id, std::move(batch_message));
    }
  }

  // Notifications are best effort. Drop messages that were not dispatched before shutdown.
  // Drain under thread_mutex: producers check is_stop_ under the same mutex, so no
  // message can be enqueued after this drain.
  {
    std::lock_guard<std::mutex> lock(dispatcher_.thread_mutex);
    while (dispatcher_.messages.Dequeue(message)) {
    }
  }
}

void NotifyBuddy::LaunchOrParkMessage(uint64_t mds_id, BatchMessage&& batch_message) {
  bool launch = false;
  {
    std::lock_guard<std::mutex> lock(dispatcher_.parked_mutex);
    auto& entry = dispatcher_.parked_messages[mds_id];
    if (entry.inflight >= FLAGS_mds_notify_message_max_inflight_per_mds) {
      entry.messages.insert(entry.messages.end(), std::make_move_iterator(batch_message.begin()),
                            std::make_move_iterator(batch_message.end()));
    } else {
      ++entry.inflight;
      launch = true;
    }
  }

  if (launch) LaunchSendMessage(mds_id, std::move(batch_message));
}

void NotifyBuddy::LaunchSendMessage(uint64_t mds_id, BatchMessage&& batch_message) {
  struct Params {
    NotifyBuddy* self{nullptr};
    uint64_t mds_id;
    BatchMessage batch_message;
  };

  sending_tasks_.fetch_add(1, std::memory_order_acq_rel);

  Params* params = new Params({.self = this, .mds_id = mds_id, .batch_message = std::move(batch_message)});

  bthread_t tid;
  bthread_attr_t attr = BTHREAD_ATTR_NORMAL;
  if (bthread_start_background(
          &tid, &attr,
          [](void* arg) -> void* {
            Params* params = reinterpret_cast<Params*>(arg);

            for (;;) {
              params->self->SendMessage(params->mds_id, params->batch_message);

              BatchMessage next_batch_message;
              if (!params->self->TakeParkedMessages(params->mds_id, next_batch_message)) break;
              params->batch_message = std::move(next_batch_message);
            }

            params->self->FinishSendTask();
            delete params;

            return nullptr;
          },
          params) != 0) {
    delete params;

    sending_tasks_.fetch_sub(1, std::memory_order_acq_rel);

    LOG(FATAL) << "[notify] start background thread fail.";
  }
}

bool NotifyBuddy::TakeParkedMessages(uint64_t mds_id, BatchMessage& batch_message) {
  std::lock_guard<std::mutex> lock(dispatcher_.parked_mutex);
  auto it = dispatcher_.parked_messages.find(mds_id);
  CHECK(it != dispatcher_.parked_messages.end()) << fmt::format("[notify.{}] parked message entry not found.", mds_id);

  auto& entry = it->second;
  if (is_stop_.load(std::memory_order_acquire) || entry.messages.empty()) {
    CHECK(entry.inflight > 0) << fmt::format("[notify.{}] in-flight count underflow.", mds_id);
    --entry.inflight;
    if (entry.inflight == 0) dispatcher_.parked_messages.erase(it);
    return false;
  }

  const size_t batch_size = FLAGS_mds_notify_message_batch_size;
  if (entry.messages.size() <= batch_size) {
    batch_message.swap(entry.messages);
  } else {
    batch_message.assign(std::make_move_iterator(entry.messages.begin()),
                         std::make_move_iterator(entry.messages.begin() + batch_size));
    entry.messages.erase(entry.messages.begin(), entry.messages.begin() + batch_size);
  }

  return true;
}

void NotifyBuddy::FinishSendTask() { sending_tasks_.fetch_sub(1, std::memory_order_acq_rel); }

void NotifyBuddy::SendMessage(uint64_t mds_id, BatchMessage& batch_message) {
  pb::mds::NotifyBuddyRequest notify_message;
  notify_message.set_id(id_generator_.fetch_add(1, std::memory_order_relaxed));

  for (auto& message : batch_message) {
    auto* mut_message = notify_message.add_messages();
    mut_message->set_fs_id(message->fs_id);
    mut_message->set_reason(message->reason);

    switch (message->type) {
      case Type::kRefreshFsInfo: {
        mut_message->set_type(pb::mds::NotifyBuddyRequest::TYPE_REFRESH_FS_INFO);

        auto refresh_fs_info_message = std::dynamic_pointer_cast<RefreshFsInfoMessage>(message);
        mut_message->mutable_refresh_fs_info()->set_fs_name(refresh_fs_info_message->fs_name);

        LOG(INFO) << fmt::format("[notify.{}] refresh fs({}/{}) info.", mds_id, message->fs_id,
                                 refresh_fs_info_message->fs_name);

      } break;

      case Type::kRefreshInode: {
        mut_message->set_type(pb::mds::NotifyBuddyRequest::TYPE_REFRESH_INODE);

        auto* mut_refresh_inode = mut_message->mutable_refresh_inode();
        auto refresh_inode_message = std::dynamic_pointer_cast<RefreshInodeMessage>(message);
        mut_refresh_inode->mutable_inode()->Swap(&refresh_inode_message->attr);
        mut_refresh_inode->mutable_attr_mutation()->Swap(&refresh_inode_message->mutation);

        LOG_DEBUG << fmt::format("[notify.{}] refresh inode, inode({}).", mds_id,
                                 mut_refresh_inode->inode().ShortDebugString());

      } break;

      case Type::kCleanPartitionCache: {
        mut_message->set_type(pb::mds::NotifyBuddyRequest::TYPE_CLEAN_PARTITION_CACHE);

        auto clean_partition_cache_message = std::dynamic_pointer_cast<CleanPartitionCacheMessage>(message);
        mut_message->mutable_clean_partition_cache()->set_ino(clean_partition_cache_message->ino);

        LOG(INFO) << fmt::format("[notify.{}] clean partition cache({}/{}) info.", mds_id, message->fs_id,
                                 clean_partition_cache_message->ino);

      } break;

      case Type::kSetDirQuota: {
        mut_message->set_type(pb::mds::NotifyBuddyRequest::TYPE_SET_DIR_QUOTA);

        auto set_dir_quota_message = std::dynamic_pointer_cast<SetDirQuotaMessage>(message);
        mut_message->mutable_set_dir_quota()->set_ino(set_dir_quota_message->ino);
        mut_message->mutable_set_dir_quota()->mutable_quota()->Swap(&set_dir_quota_message->quota);

        LOG(INFO) << fmt::format("[notify.{}] set dir quota, dir({}/{}) quota({}).", mds_id, message->fs_id,
                                 set_dir_quota_message->ino, set_dir_quota_message->quota.ShortDebugString());

      } break;

      case Type::kDeleteDirQuota: {
        mut_message->set_type(pb::mds::NotifyBuddyRequest::TYPE_DELETE_DIR_QUOTA);

        auto delete_dir_quota_message = std::dynamic_pointer_cast<DeleteDirQuotaMessage>(message);
        mut_message->mutable_delete_dir_quota()->set_ino(delete_dir_quota_message->ino);
        mut_message->mutable_delete_dir_quota()->set_uuid(delete_dir_quota_message->uuid);

        LOG(INFO) << fmt::format("[notify.{}] delete dir quota, dir({}/{}/{}).", mds_id, message->fs_id,
                                 delete_dir_quota_message->ino, delete_dir_quota_message->uuid);

      } break;

      default:
        LOG(FATAL) << fmt::format("[notify] unknown message type: {}.", static_cast<int>(message->type));
        break;
    }
  }

  butil::EndPoint endpoint;
  if (!GenEndpoint(mds_id, endpoint)) {
    LOG(ERROR) << fmt::format("[notify.{}] gen endpoint fail.", mds_id);
    return;
  }

  auto status = ServiceAccess::NotifyBuddy(endpoint, notify_message);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("[notify.{}] send message fail, {}.", mds_id, status.error_str());
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