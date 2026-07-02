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
#include "client/vfs/compaction/compactor.h"
#include "client/vfs/components/file_suffix_watcher.h"
#include "client/vfs/components/prefetch_manager.h"
#include "client/vfs/components/uid_gid_mapper.h"
#include "client/vfs/components/warmup_manager.h"
#include "client/vfs/handle/handle_manager.h"
#include "client/vfs/metasystem/meta_wrapper.h"
#include "client/vfs/vfs.h"
#include "client/vfs/vfs_meta.h"
#include "common/blockaccess/block_accesser.h"
#include "common/readmempool/read_mem_pool.h"
#include "common/readmempool/read_mem_pool_vars.h"
#include "common/status.h"
#include "common/trace/trace_manager.h"
#include "common/writemempool/write_mem_pool.h"
#include "utils/executor/executor.h"
#include "utils/logclean_manager.h"

namespace dingofs {
namespace client {
namespace vfs {

class WriterTable;  // forward decl; full include lives in vfs_hub.cc

class VFSHub {
 public:
  VFSHub() = default;

  virtual ~VFSHub() = default;

  virtual Status Start(bool skip_mount) = 0;

  virtual Status Stop(bool skip_unmount) = 0;

  virtual ClientId GetClientId() = 0;

  virtual MetaWrapper* GetMetaSystem() = 0;

  virtual HandleManager* GetHandleManager() = 0;

  virtual WriterTable* GetWriterTable() = 0;

  virtual BlockStore* GetBlockStore() = 0;

  virtual blockaccess::BlockAccesser* GetBlockAccesser() = 0;

  virtual Executor* GetReadExecutor() = 0;

  virtual Executor* GetReadCleanupExecutor() = 0;

  virtual Executor* GetWriteBackgroundExecutor() = 0;

  virtual Executor* GetFlushExecutor() = 0;

  virtual Executor* GetCBExecutor() = 0;

  virtual WriteMemPool* GetWriteMemPool() = 0;

  virtual ReadMemPool* GetReadMemPool() = 0;

  virtual FileSuffixWatcher* GetFileSuffixWatcher() = 0;

  virtual PrefetchManager* GetPrefetchManager() = 0;

  virtual WarmupManager* GetWarmupManager() = 0;

  virtual Compactor* GetCompactor() = 0;

  virtual TraceManager* GetTraceManager() = 0;

  virtual FsInfo GetFsInfo() = 0;

  virtual blockaccess::BlockAccessOptions GetBlockAccesserOptions() = 0;

  virtual UidGidMapper* GetUidGidMapper() = 0;
};

class VFSHubImpl : public VFSHub {
 public:
  VFSHubImpl(const VFSConfig& vfs_conf, ClientId client_id);

  ~VFSHubImpl() override;

  Status Start(bool skip_mount) override;

  Status Stop(bool skip_unmount) override;

  ClientId GetClientId() override { return client_id_; }

  MetaWrapper* GetMetaSystem() override {
    CHECK_NOTNULL(meta_wrapper_);
    return meta_wrapper_.get();
  }

  HandleManager* GetHandleManager() override {
    CHECK_NOTNULL(handle_manager_);
    return handle_manager_.get();
  }

  WriterTable* GetWriterTable() override;  // out-of-line (see vfs_hub.cc)

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

  Executor* GetReadCleanupExecutor() override {
    CHECK_NOTNULL(read_cleanup_executor_);
    return read_cleanup_executor_.get();
  }

  Executor* GetWriteBackgroundExecutor() override {
    CHECK_NOTNULL(write_background_executor_);
    return write_background_executor_.get();
  }

  Executor* GetFlushExecutor() override {
    CHECK_NOTNULL(flush_executor_);
    return flush_executor_.get();
  }

  Executor* GetCBExecutor() override {
    CHECK_NOTNULL(cb_executor_);
    return cb_executor_.get();
  }

  WriteMemPool* GetWriteMemPool() override {
    CHECK_NOTNULL(write_buffer_manager_);
    return write_buffer_manager_.get();
  }

  ReadMemPool* GetReadMemPool() override {
    CHECK_NOTNULL(read_mem_pool_);
    return read_mem_pool_.get();
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

  Compactor* GetCompactor() override {
    CHECK_NOTNULL(compactor_);
    return compactor_.get();
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

  UidGidMapper* GetUidGidMapper() override {
    CHECK_NOTNULL(uid_gid_mapper_);
    return uid_gid_mapper_.get();
  }

 private:
  // started_ means "serviceable to FUSE ops" (set when Start completes, cleared
  // when Stop begins). stopped_ is the teardown-needed gate: it starts true (a
  // freshly constructed, never-started hub owns nothing to tear down), Start()
  // arms it (false) before creating any component, and Stop() disarms it (back
  // to true) exactly once. So the destructor's Stop() tears down a half-started
  // hub yet is a clean no-op for a never-started or already-stopped one,
  // without relying on the "no Start() after Stop()" invariant.
  std::atomic_bool started_{false};
  std::atomic_bool stopped_{true};

  blockaccess::BlockAccessOptions blockaccess_options_;

  const ClientId client_id_;
  const VFSConfig vfs_conf_;

  FsInfo fs_info_;
  S3Info s3_info_;

  std::unique_ptr<TraceManager> trace_manager_;
  std::unique_ptr<Compactor> compactor_;
  std::unique_ptr<MetaWrapper> meta_wrapper_;
  std::unique_ptr<WriterTable> writer_table_;
  std::unique_ptr<HandleManager> handle_manager_;
  std::unique_ptr<blockaccess::BlockAccesser> block_accesser_;
  std::unique_ptr<BlockStore> block_store_;
  std::unique_ptr<Executor> read_executor_;

  // Reader-local cleanup only: periodic shrink and read-request cleanup.
  // It must not issue block_store I/O.  Keep it alive until after
  // block_store_->Shutdown(), because cache bthread read completions can still
  // schedule cleanup work while block_store is draining.
  std::unique_ptr<Executor> read_cleanup_executor_;

  // Writer-side background work: periodic flush scheduling and slice-id
  // pre-allocation.  These tasks may touch flush_executor, block_store, and
  // meta_system, so Stop() must drain this executor before those dependencies
  // are torn down.
  std::unique_ptr<Executor> write_background_executor_;

  std::unique_ptr<Executor> flush_executor_;
  std::unique_ptr<Executor> cb_executor_;
  std::unique_ptr<WriteMemPool> write_buffer_manager_;
  std::unique_ptr<ReadMemPool> read_mem_pool_;
  std::unique_ptr<ReadMemPoolVars>
      read_mem_pool_vars_;  // after pool: dtor first
  std::unique_ptr<FileSuffixWatcher> file_suffix_watcher_;
  std::unique_ptr<PrefetchManager> prefetch_manager_;
  std::unique_ptr<WarmupManager> warmup_manager_;
  std::unique_ptr<utils::LogCleanManager> logclean_manager_;
  // Owns its own /etc passwd/group inotify watch internally (see Init).
  std::unique_ptr<UidGidMapper> uid_gid_mapper_;
};

}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_CLIENT_VFS_HUB_VFS_HUB_H_
