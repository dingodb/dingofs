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

#include "client/vfs/hub/vfs_hub.h"

#include <fmt/format.h>
#include <glog/logging.h>

#include <atomic>
#include <memory>

#include "client/vfs/blockstore/block_store_impl.h"
#include "client/vfs/blockstore/fake_block_store.h"
#include "client/vfs/common/helper.h"
#include "client/vfs/components/prefetch_manager.h"
#include "client/vfs/components/warmup_manager.h"
#include "client/vfs/metasystem/local/metasystem.h"
#include "client/vfs/metasystem/mds/metasystem.h"
#include "client/vfs/metasystem/memory/metasystem.h"
#include "client/vfs/metasystem/meta_wrapper.h"
#include "client/vfs/vfs.h"
#include "client/vfs/vfs_meta.h"
#include "common/blockaccess/accesser_common.h"
#include "common/blockaccess/block_accesser.h"
#include "common/blockaccess/rados/rados_common.h"
#include "common/const.h"
#include "common/options/client.h"
#include "common/status.h"
#include "utils/executor/thread/executor_impl.h"

namespace dingofs {
namespace client {
namespace vfs {

static const std::string kFlushExecutorName = "vfs_flush";
static const std::string kReadExecutorName = "vfs_read";
static const std::string kBgExecutorName = "vfs_bg";
static const std::string kCBExecutorName = "vfs_callback";

static MetaSystemUPtr BuildMetaSystem(const VFSConfig& vfs_conf,
                                      const ClientId& client_id,
                                      TraceManager& trace_manager,
                                      Compactor& compactor) {
  if (vfs_conf.metasystem_type == MetaSystemType::MEMORY) {
    return std::make_unique<memory::MemoryMetaSystem>();

  } else if (vfs_conf.metasystem_type == MetaSystemType::LOCAL) {
    return std::make_unique<local::LocalMetaSystem>(
        dingofs::Helper::ExpandPath(kDefaultMetaDBDir), vfs_conf.fs_name,
        vfs_conf.storage_info);

  } else if (vfs_conf.metasystem_type == MetaSystemType::MDS) {
    return meta::MDSMetaSystem::Build(vfs_conf.fs_name, vfs_conf.mds_addrs,
                                      client_id, trace_manager, compactor);
  }

  return nullptr;
}

VFSHubImpl::VFSHubImpl(const VFSConfig& vfs_conf, ClientId client_id)
    : client_id_(client_id), vfs_conf_(vfs_conf) {}

VFSHubImpl::~VFSHubImpl() {
  if (handle_manager_ != nullptr) {
    handle_manager_.reset();
  }

  if (read_executor_ != nullptr) {
    read_executor_.reset();
  }

  if (flush_executor_ != nullptr) {
    flush_executor_.reset();
  }

  if (bg_executor_ != nullptr) {
    bg_executor_.reset();
  }

  if (warmup_manager_ != nullptr) {
    warmup_manager_.reset();
  }

  if (prefetch_manager_ != nullptr) {
    prefetch_manager_.reset();
  }

  if (block_store_ != nullptr) {
    block_store_.reset();
  }

  if (meta_wrapper_ != nullptr) {
    meta_wrapper_.reset();
  }

  if (compactor_ != nullptr) {
    compactor_.reset();
  }

  if (trace_manager_ != nullptr) {
    trace_manager_.reset();
  }
}

Status VFSHubImpl::Start(bool upgrade) {
  CHECK(started_.load(std::memory_order_relaxed) == false)
      << "unexpected start";

  LOG(INFO) << fmt::format("[vfs.hub] vfs hub starting, upgrade({}).", upgrade);

  // trace manager
  trace_manager_ = std::make_unique<TraceManager>();
  if (!trace_manager_->Init()) {
    return Status::Internal("init trace manager fail");
  }

  compactor_ = std::make_unique<Compactor>(this);

  // meta system
  {
    auto meta =
        BuildMetaSystem(vfs_conf_, client_id_, *trace_manager_, *compactor_);
    if (meta == nullptr) {
      return Status::Internal("build meta system fail");
    }

    meta_wrapper_ = std::make_unique<MetaWrapper>(std::move(meta));
    DINGOFS_RETURN_NOT_OK(meta_wrapper_->Init(upgrade));
  }

  // load fs info
  {
    auto span = trace_manager_->StartSpan("vfs::start");

    DINGOFS_RETURN_NOT_OK(
        meta_wrapper_->GetFsInfo(SpanScope::GetContext(span), &fs_info_));

    LOG(INFO) << fmt::format("[vfs.hub] vfs_fs_info: {}", FsInfo2Str(fs_info_));
    if (fs_info_.status != FsStatus::kNormal) {
      return Status::Internal(fmt::format("fs is unavailable, status({})",
                                          FsStatus2Str(fs_info_.status)));
    }
  }

  // block accesser
  {
    // set s3/rados config info
    blockaccess::InitBlockAccessOption(&blockaccess_options_);
    if (fs_info_.storage_info.store_type == StoreType::kS3) {
      auto s3_info = fs_info_.storage_info.s3_info;
      blockaccess_options_.type = blockaccess::AccesserType::kS3;
      blockaccess_options_.s3_options.s3_info =
          blockaccess::S3Info{.ak = s3_info.ak,
                              .sk = s3_info.sk,
                              .endpoint = s3_info.endpoint,
                              .bucket_name = s3_info.bucket_name};

    } else if (fs_info_.storage_info.store_type == StoreType::kRados) {
      auto rados_info = fs_info_.storage_info.rados_info;
      blockaccess_options_.type = blockaccess::AccesserType::kRados;
      blockaccess_options_.rados_options =
          blockaccess::RadosOptions{.mon_host = rados_info.mon_host,
                                    .user_name = rados_info.user_name,
                                    .key = rados_info.key,
                                    .pool_name = rados_info.pool_name,
                                    .cluster_name = rados_info.cluster_name};
    } else if (fs_info_.storage_info.store_type == StoreType::kLocalFile) {
      blockaccess_options_.type = blockaccess::AccesserType::kLocalFile;
      blockaccess_options_.file_options = blockaccess::LocalFileOptions{
          .path = fs_info_.storage_info.file_info.path};
    } else {
      return Status::InvalidParam("unsupported store type");
    }

    block_accesser_ = blockaccess::NewBlockAccesser(blockaccess_options_);
    DINGOFS_RETURN_NOT_OK(block_accesser_->Init());
  }

  // handle manager
  {
    handle_manager_ = std::make_unique<HandleManager>(this);
    CHECK(handle_manager_ != nullptr) << "handle manager is nullptr.";
    DINGOFS_RETURN_NOT_OK(handle_manager_->Start());
  }

  // block store
  {
    if (FLAGS_vfs_use_fake_block_store) {
      block_store_ = std::make_unique<FakeBlockStore>(this, fs_info_.uuid);
    } else {
      block_store_ = std::make_unique<BlockStoreImpl>(this, fs_info_.uuid,
                                                      block_accesser_.get());
    }
    CHECK(block_store_ != nullptr) << "block store is nullptr.";
    DINGOFS_RETURN_NOT_OK(block_store_->Start());
  }

  {
    read_executor_ = std::make_unique<ExecutorImpl>(
        kReadExecutorName, FLAGS_vfs_read_executor_thread);
    if (!read_executor_->Start()) {
      return Status::Internal("read executor start fail");
    }
  }

  {
    bg_executor_ = std::make_unique<ExecutorImpl>(kBgExecutorName,
                                                  FLAGS_vfs_bg_executor_thread);
    if (!bg_executor_->Start()) {
      return Status::Internal("bg executor start fail");
    }
  }

  {
    flush_executor_ = std::make_unique<ExecutorImpl>(kFlushExecutorName,
                                                     FLAGS_vfs_flush_thread);
    if (!flush_executor_->Start()) {
      return Status::Internal("flush executor start fail");
    }
  }

  {
    cb_executor_ =
        std::make_unique<ExecutorImpl>(kCBExecutorName, FLAGS_vfs_cb_thread);
    if (!cb_executor_->Start()) {
      return Status::Internal("callback executor start fail");
    }
  }

  write_buffer_manager_ = std::make_unique<WriteBufferManager>(
      FLAGS_vfs_write_buffer_total_mb * 1024 * 1024,
      FLAGS_vfs_write_buffer_page_size);

  // read buffer manager
  {
    int64_t total_bytes = FLAGS_vfs_read_buffer_total_mb * 1024 * 1024;
    if (total_bytes <= 0) {
      return Status::Internal("invalid vfs_read_buffer_total_mb");
    }

    read_buffer_manager_ = std::make_unique<ReadBufferManager>(total_bytes);
  }

  file_suffix_watcher_ =
      std::make_unique<FileSuffixWatcher>(FLAGS_vfs_data_writeback_suffix);

  // prefetch manager
  {
    if (block_store_->EnableCache()) {
      prefetch_manager_ = PrefetchManager::New(this);
      CHECK(prefetch_manager_ != nullptr) << "prefetch manager is nullptr.";
      DINGOFS_RETURN_NOT_OK(
          prefetch_manager_->Start(FLAGS_vfs_prefetch_threads));

    } else {
      LOG(INFO)
          << "[vfs.hub] block cache not enable, skip prefetch manager start.";
    }
  }

  // warmup manager
  {
    warmup_manager_ = WarmupManager::New(this);
    CHECK(warmup_manager_ != nullptr) << "warmup manager is nullptr.";
    DINGOFS_RETURN_NOT_OK(warmup_manager_->Start(FLAGS_vfs_warmup_threads));
  }

  started_.store(true, std::memory_order_relaxed);

  return Status::OK();
}

Status VFSHubImpl::Stop(bool upgrade) {
  if (!started_.load(std::memory_order_relaxed)) {
    return Status::OK();
  }

  LOG(INFO) << fmt::format("[vfs.hub] stopping vfs hub, upgrade({}).", upgrade);

  if (handle_manager_ != nullptr) {
    handle_manager_->Stop();
  }

  if (read_executor_ != nullptr) {
    read_executor_->Stop();
  }

  if (bg_executor_ != nullptr) {
    bg_executor_->Stop();
  }

  if (flush_executor_ != nullptr) {
    flush_executor_->Stop();
  }

  if (warmup_manager_ != nullptr) {
    warmup_manager_->Stop();
  }

  if (prefetch_manager_ != nullptr) {
    prefetch_manager_->Stop();
  }

  if (block_store_ != nullptr) {
    block_store_->Shutdown();
  }

  if (meta_wrapper_ != nullptr) {
    meta_wrapper_->Stop(upgrade);
  }

  if (trace_manager_ != nullptr) {
    trace_manager_->Stop();
  }

  if (cb_executor_ != nullptr) {
    cb_executor_->Stop();
  }

  LOG(INFO) << fmt::format("[vfs.hub] stopped vfs hub, upgrade({}).", upgrade);

  started_.store(false, std::memory_order_relaxed);

  return Status::OK();
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs
