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

#ifndef DINGOFS_CLIENT_VFS_HUB_VFS_HUB_H_
#define DINGOFS_CLIENT_VFS_HUB_VFS_HUB_H_

#include <glog/logging.h>

#include <atomic>
#include <memory>

#include "client/vfs/blockstore/block_store.h"
#include "client/vfs/common/client_id.h"
#include "client/vfs/components/file_suffix_watcher.h"
#include "client/vfs/components/prefetch_manager.h"
#include "client/vfs/components/warmup_manager.h"
#include "client/vfs/handle/handle_manager.h"
#include "client/vfs/memory/read_buffer_manager.h"
#include "client/vfs/memory/write_buffer_manager.h"
#include "client/vfs/metasystem/meta_system.h"
#include "client/vfs/vfs.h"
#include "client/vfs/vfs_meta.h"
#include "common/blockaccess/block_accesser.h"
#include "common/status.h"
#include "common/trace/trace_manager.h"
#include "utils/executor/executor.h"

namespace dingofs {
namespace client {
namespace vfs {

class VFSHub {
 public:
  VFSHub() = default;

  virtual ~VFSHub() = default;

  virtual Status Start(const VFSConfig& vfs_conf, bool upgrade) = 0;

  virtual Status Stop(bool upgrade) = 0;

  virtual ClientId GetClientId() = 0;

  virtual MetaSystem* GetMetaSystem() = 0;

  virtual HandleManager* GetHandleManager() = 0;

  virtual BlockStore* GetBlockStore() = 0;

  virtual blockaccess::BlockAccesser* GetBlockAccesser() = 0;

  virtual Executor* GetReadExecutor() = 0;

  virtual Executor* GetBGExecutor() = 0;

  virtual Executor* GetFlushExecutor() = 0;

  virtual WriteBufferManager* GetWriteBufferManager() = 0;

  virtual ReadBufferManager* GetReadBufferManager() = 0;

  virtual FileSuffixWatcher* GetFileSuffixWatcher() = 0;

  virtual PrefetchManager* GetPrefetchManager() = 0;

  virtual WarmupManager* GetWarmupManager() = 0;

  virtual FsInfo GetFsInfo() = 0;

  virtual TraceManager* GetTraceManager() = 0;

  virtual blockaccess::BlockAccessOptions GetBlockAccesserOptions() = 0;
};

class VFSHubImpl : public VFSHub {
 public:
  VFSHubImpl(ClientId client_id) : client_id_(client_id) {}

  ~VFSHubImpl() override;

  Status Start(const VFSConfig& vfs_conf, bool upgrade) override;

  Status Stop(bool upgrade) override;

  ClientId GetClientId() override { return client_id_; }

  MetaSystem* GetMetaSystem() override {
    CHECK_NOTNULL(meta_system_);
    return meta_system_.get();
  }

  HandleManager* GetHandleManager() override {
    CHECK_NOTNULL(handle_manager_);
    return handle_manager_.get();
  }

  BlockStore* GetBlockStore() override {
    CHECK_NOTNULL(block_store_);
    return block_store_.get();
  }

  blockaccess::BlockAccesser* GetBlockAccesser() override {
    CHECK_NOTNULL(block_accesser_);
    return block_accesser_.get();
  }

  Executor* GetReadExecutor() override {
    CHECK_NOTNULL(read_executor_);
    return read_executor_.get();
  }

  Executor* GetBGExecutor() override {
    CHECK_NOTNULL(bg_executor_);
    return bg_executor_.get();
  }

  Executor* GetFlushExecutor() override {
    CHECK_NOTNULL(flush_executor_);
    return flush_executor_.get();
  }

  WriteBufferManager* GetWriteBufferManager() override {
    CHECK_NOTNULL(write_buffer_manager_);
    return write_buffer_manager_.get();
  }

  ReadBufferManager* GetReadBufferManager() override {
    CHECK_NOTNULL(read_buffer_manager_);
    return read_buffer_manager_.get();
  }

  FileSuffixWatcher* GetFileSuffixWatcher() override {
    CHECK_NOTNULL(file_suffix_watcher_);
    return file_suffix_watcher_.get();
  }

  PrefetchManager* GetPrefetchManager() override {
    CHECK_NOTNULL(prefetch_manager_);
    return prefetch_manager_.get();
  }

  WarmupManager* GetWarmupManager() override {
    CHECK_NOTNULL(warmup_manager_);
    return warmup_manager_.get();
  }

  FsInfo GetFsInfo() override {
    CHECK(started_.load(std::memory_order_relaxed)) << "not started";
    return fs_info_;
  }

  TraceManager* GetTraceManager() override {
    CHECK_NOTNULL(trace_manager_);
    return trace_manager_.get();
  }

  blockaccess::BlockAccessOptions GetBlockAccesserOptions() override {
    CHECK(started_.load(std::memory_order_relaxed)) << "not started";
    return blockaccess_options_;
  }

 private:
  std::atomic_bool started_{false};

  blockaccess::BlockAccessOptions blockaccess_options_;

  const ClientId client_id_;

  FsInfo fs_info_;
  S3Info s3_info_;

  std::unique_ptr<MetaSystem> meta_system_;
  std::unique_ptr<HandleManager> handle_manager_;
  std::unique_ptr<blockaccess::BlockAccesser> block_accesser_;
  std::unique_ptr<BlockStore> block_store_;
  std::unique_ptr<Executor> read_executor_;
  std::unique_ptr<Executor> bg_executor_;
  std::unique_ptr<Executor> flush_executor_;
  std::unique_ptr<WriteBufferManager> write_buffer_manager_;
  std::unique_ptr<ReadBufferManager> read_buffer_manager_;
  std::unique_ptr<FileSuffixWatcher> file_suffix_watcher_;
  std::unique_ptr<PrefetchManager> prefetch_manager_;
  std::unique_ptr<WarmupManager> warmup_manager_;
  std::shared_ptr<TraceManager> trace_manager_;
};

}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_CLIENT_VFS_HUB_VFS_HUB_H_
