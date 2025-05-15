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
 * Created Date: 2025-05-13
 * Author: Jingli Chen (Wine93)
 */

#include "cache/storage/storage.h"

#include <bthread/condition_variable.h>
#include <bthread/mutex.h>
#include <butil/iobuf.h>
#include <glog/logging.h>
#include <sys/types.h>

#include <cstddef>
#include <memory>
#include <mutex>

#include "cache/storage/buffer.h"
#include "cache/storage/closure.h"

namespace dingofs {
namespace cache {
namespace storage {

enum class OperatorType : uint8_t {
  kPut = 0,
  kGet = 1,
};

class StorageImpl::StorageClosure : public Closure {
 public:
  StorageClosure(OperatorType optype, const std::string& key, off_t offset,
                 size_t length, IOBuffer* buffer)
      : optype_(optype),
        key_(key),
        offset_(offset),
        length_(length),
        buffer_(buffer) {}

  void Run() override {
    {
      std::unique_lock<bthread::Mutex> lk(mutex_);
      cond_.notify_all();
    }
    delete this;
  }

  void Wait() {
    std::unique_lock<bthread::Mutex> lk(mutex_);
    cond_.wait(lk);
  }

 private:
  friend class StorageImpl;

 private:
  OperatorType optype_;
  std::string key_;
  off_t offset_;
  size_t length_;
  IOBuffer* buffer_;
  bthread::Mutex mutex_;
  bthread::ConditionVariable cond_;
};

StorageImpl::StorageImpl(dataaccess::DataAccesserPtr data_accesser)
    : running_(false),
      data_accesser_(data_accesser),
      task_thread_pool_(std::make_unique<dingofs::utils::TaskThreadPool<>>(
          "cache_storage_worker")) {}

Status StorageImpl::Init() {
  if (!running_.exchange(true)) {
    int rc = task_thread_pool_->Start(3);
    if (rc != 0) {
      LOG(ERROR) << "Start storage thread pool failed: rc = " << rc;
      return Status::Internal("start storage thread pool fail");
    }

    bthread::ExecutionQueueOptions queue_options;
    queue_options.use_pthread = true;
    rc = bthread::execution_queue_start(&submit_queue_id_, &queue_options,
                                        Executor, this);
    if (rc != 0) {
      LOG(ERROR) << "Start storage submit queue failed: rc = " << rc;
      return Status::Internal("start storage submit queue fail");
    }
  }
  return Status::OK();
}

Status StorageImpl::Shutdown() {
  if (running_.exchange(false)) {
    bthread::execution_queue_stop(submit_queue_id_);
    int rc = bthread::execution_queue_join(submit_queue_id_);
    if (rc != 0) {
      LOG(ERROR) << "Stop storage submit queue failed: rc = " << rc;
      return Status::Internal("stop storage submit queue failed");
    }
    task_thread_pool_->Stop();
  }
  return Status::OK();
}

Status StorageImpl::Put(const std::string& key, IOBuffer* buffer) {
  auto* closure =
      new StorageClosure(OperatorType::kPut, key, 0, buffer->Size(), buffer);
  CHECK_EQ(0, bthread::execution_queue_execute(submit_queue_id_, closure));
  closure->Wait();
  return closure->status();
}

Status StorageImpl::Get(const std::string& key, off_t offset, size_t length,
                        IOBuffer* buffer) {
  auto* closure =
      new StorageClosure(OperatorType::kGet, key, offset, length, buffer);
  CHECK_EQ(0, bthread::execution_queue_execute(submit_queue_id_, closure));
  closure->Wait();
  return closure->status();
}

int StorageImpl::Executor(void* meta,
                          bthread::TaskIterator<StorageClosure*>& iter) {
  if (iter.is_queue_stopped()) {
    return 0;
  }

  StorageImpl* storage = static_cast<StorageImpl*>(meta);
  for (; iter; iter++) {
    auto* closure = *iter;
    storage->task_thread_pool_->Enqueue([storage, closure]() {
      if (closure->optype_ == OperatorType::kPut) {
        storage->Put(closure);
      } else {
        storage->Get(closure);
      }
    });
  }
  return 0;
}

namespace helper {

using BufferDeleter = std::function<void(char*)>;

std::pair<char*, BufferDeleter> NewContinuousBuffer(IOBuffer* buffer) {
  char* data;
  BufferDeleter deleter;
  auto bufvec = buffer->Buffers();
  if (bufvec.size() == 1) {
    data = bufvec[0].data;
    deleter = [](char* data) {};
  } else {
    data = new char[buffer->Size()];
    buffer->CopyTo(data);
    deleter = [](const char* data) { delete[] data; };
  }
  return std::make_pair(data, deleter);
}

};  // namespace helper

void StorageImpl::Put(StorageClosure* closure) {
  auto continous_buffer = helper::NewContinuousBuffer(closure->buffer_);
  char* data = continous_buffer.first;
  auto deleter = continous_buffer.second;

  auto retry_cb = [closure, data, deleter](int code) {
    if (code == 0) {  // success
      deleter(data);
      closure->Run();
      return false;
    }

    LOG(ERROR) << "Put object to storage failed: key = " << closure->key_
               << ", rc = " << code;
    return true;  // retry forever
  };

  data_accesser_->AsyncPut(closure->key_, data, closure->length_, retry_cb);
}

void StorageImpl::Get(StorageClosure* closure) {
  char* data = new char[closure->length_];

  auto retry_cb = [closure, data](int code) {
    if (code == 0) {  // success
      butil::IOBuf iobuf;
      iobuf.append_user_data(data, closure->length_, [](void* data) {
        delete[] static_cast<char*>(data);
      });
      *closure->buffer_ = IOBuffer(iobuf);
      closure->Run();
      return false;
    }

    LOG(ERROR) << "Get object from storage failed: key = " << closure->key_
               << ", rc = " << code;
    delete[] data;
    return false;  // never retry
  };

  data_accesser_->AsyncGet(closure->key_, closure->offset_, closure->length_,
                           data, retry_cb);
}

}  // namespace storage
}  // namespace cache
}  // namespace dingofs
