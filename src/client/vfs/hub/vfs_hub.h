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

#ifndef DINGOFS_CLIENT_VFS_VFS_HUB_H_
#define DINGOFS_CLIENT_VFS_VFS_HUB_H_

#include <glog/logging.h>

#include <atomic>
#include <cstdint>
#include <memory>

#include "blockaccess/block_accesser.h"
#include "cache/blockcache/block_cache.h"
#include "client/memory/page_allocator.h"
#include "options/client/options/vfs/vfs_option.h"
#include "client/vfs.h"
#include "client/vfs/background/iperiodic_flush_manager.h"
#include "client/vfs/handle/handle_manager.h"
#include "client/vfs/meta/meta_system.h"
#include "client/vfs/vfs_meta.h"
#include "common/status.h"
#include "utils/executor/executor.h"

namespace dingofs {
namespace client {
namespace vfs {

class VFSHub {
 public:
  VFSHub() = default;

  virtual ~VFSHub() = default;

  virtual Status Start(const VFSConfig& vfs_conf,
                       const VFSOption& vfs_option) = 0;

  virtual Status Stop() = 0;

  virtual MetaSystem* GetMetaSystem() = 0;

  virtual HandleManager* GetHandleManager() = 0;

  //   virtual cache::BlockCache* GetBlockCache() = 0;
  virtual std::shared_ptr<cache::BlockCache> GetBlockCache() = 0;

  virtual blockaccess::BlockAccesser* GetBlockAccesser() = 0;

  virtual Executor* GetFlushExecutor() = 0;

  virtual IPeriodicFlushManager* GetPeriodicFlushManger() = 0;

  virtual datastream::PageAllocator* GetPageAllocator() = 0;

  virtual FsInfo GetFsInfo() = 0;

  virtual uint64_t GetPageSize() = 0;
};

class VFSHubImpl : public VFSHub {
 public:
  VFSHubImpl() = default;

  ~VFSHubImpl() override { Stop(); }

  Status Start(const VFSConfig& vfs_conf, const VFSOption& vfs_option) override;

  Status Stop() override;

  MetaSystem* GetMetaSystem() override {
    CHECK_NOTNULL(meta_system_);
    return meta_system_.get();
  }

  HandleManager* GetHandleManager() override {
    CHECK_NOTNULL(handle_manager_);
    return handle_manager_.get();
  }

  std::shared_ptr<cache::BlockCache> GetBlockCache() override {
    CHECK_NOTNULL(handle_manager_);
    return block_cache_;
    // return block_cache_.get();
  }

  //   cache::BlockCache* GetBlockCache() override {
  //     CHECK_NOTNULL(handle_manager_);
  //     return block_cache_.get();
  //   }

  blockaccess::BlockAccesser* GetBlockAccesser() override {
    CHECK_NOTNULL(block_accesser_);
    return block_accesser_.get();
  }

  Executor* GetFlushExecutor() override {
    CHECK_NOTNULL(flush_executor_);
    return flush_executor_.get();
  }

  IPeriodicFlushManager* GetPeriodicFlushManger() override {
    CHECK_NOTNULL(priodic_flush_manager_);
    return priodic_flush_manager_.get();
  }

  datastream::PageAllocator* GetPageAllocator() override {
    CHECK_NOTNULL(page_allocator_);
    return page_allocator_.get();
  }

  FsInfo GetFsInfo() override {
    CHECK(started_.load(std::memory_order_relaxed)) << "not started";
    return fs_info_;
  }

  uint64_t GetPageSize() override {
    CHECK(started_.load(std::memory_order_relaxed)) << "not started";
    return vfs_option_.data_stream_option.page_option.page_size;
  }

 private:
  std::atomic_bool started_{false};

  VFSOption vfs_option_;
  FsInfo fs_info_;
  S3Info s3_info_;
  std::unique_ptr<MetaSystem> meta_system_;
  std::unique_ptr<HandleManager> handle_manager_;
  std::unique_ptr<blockaccess::BlockAccesser> block_accesser_;
  //   std::unique_ptr<cache::BlockCache> block_cache_;
  std::shared_ptr<cache::BlockCache> block_cache_;
  std::unique_ptr<Executor> flush_executor_;
  std::unique_ptr<IPeriodicFlushManager> priodic_flush_manager_;
  std::shared_ptr<datastream::PageAllocator> page_allocator_;
};

}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_CLIENT_VFS_VFS_HUB_H_