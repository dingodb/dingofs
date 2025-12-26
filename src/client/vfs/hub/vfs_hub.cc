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

#include <atomic>
#include <cstdint>
#include <memory>

#include "client/common/const.h"
#include "client/vfs/blockstore/block_store_impl.h"
#include "client/vfs/blockstore/fake_block_store.h"
#include "client/vfs/common/helper.h"
#include "client/vfs/components/prefetch_manager.h"
#include "client/vfs/components/warmup_manager.h"
#include "client/vfs/metasystem/local/metasystem.h"
#include "client/vfs/metasystem/mds/metasystem.h"
#include "client/vfs/metasystem/memory/metasystem.h"
#include "client/vfs/metasystem/meta_system.h"
#include "client/vfs/metasystem/meta_wrapper.h"
#include "client/vfs/vfs.h"
#include "client/vfs/vfs_meta.h"
#include "common/blockaccess/accesser_common.h"
#include "common/blockaccess/block_accesser.h"
#include "common/blockaccess/rados/rados_common.h"
#include "common/const.h"
#include "common/helper.h"
#include "common/options/client.h"
#include "common/status.h"
#include "common/trace/log_trace_exporter.h"
#include "common/trace/noop_tracer.h"
#include "common/trace/tracer.h"
#include "common/types.h"
#include "fmt/format.h"
#include "glog/logging.h"
#include "utils/executor/thread/executor_impl.h"

namespace dingofs {
namespace client {
namespace vfs {

static const std::string kFlushExecutorName = "vfs_flush";
static const std::string kReadExecutorName = "vfs_read";

Status VFSHubImpl::Start(const VFSConfig& vfs_conf, bool upgrade) {
  CHECK(started_.load(std::memory_order_relaxed) == false)
      << "unexpected start";

  LOG(INFO) << fmt::format("[vfs.hub] vfs hub starting, upgrade({}).", upgrade);

  {
    trace_manager_ = TraceManager::New();
    auto ok = trace_manager_->Init();
    if (!ok) {
      LOG(ERROR) << "Init trace manager failed.";
      return Status::Internal("Init trace manager failed");
    }
  }

  MetaSystemUPtr rela_meta_system;
  if (vfs_conf.metasystem_type == MetaSystemType::MEMORY) {
    rela_meta_system = std::make_unique<memory::MemoryMetaSystem>();

  } else if (vfs_conf.metasystem_type == MetaSystemType::LOCAL) {
    rela_meta_system = std::make_unique<local::LocalMetaSystem>(
        dingofs::Helper::ExpandPath(kDefaultMetaDBDir), vfs_conf.fs_name,
        vfs_conf.storage_info);

  } else if (vfs_conf.metasystem_type == MetaSystemType::MDS) {
    rela_meta_system = v2::MDSMetaSystem::Build(
        vfs_conf.fs_name, vfs_conf.mds_addrs, client_id_, trace_manager_);
  }
  CHECK(rela_meta_system != nullptr) << "build meta system fail";

  if (FLAGS_vfs_trace_logging) {
    tracer_ = std::make_unique<Tracer>(
        std::make_unique<LogTraceExporter>(kVFSMoudule, Logger::LogDir()));
  } else {
    tracer_ = std::make_unique<NoopTracer>();
  }

  auto span = tracer_->StartSpan(kVFSMoudule, "start");

  meta_system_ = std::make_unique<MetaWrapper>(std::move(rela_meta_system));

  DINGOFS_RETURN_NOT_OK(meta_system_->Init(upgrade));

  DINGOFS_RETURN_NOT_OK(meta_system_->GetFsInfo(span->GetContext(), &fs_info_));

  LOG(INFO) << fmt::format("[vfs.hub] vfs_fs_info: {}", FsInfo2Str(fs_info_));
  if (fs_info_.status != FsStatus::kNormal) {
    LOG(ERROR) << fmt::format("[vfs.hub] fs {} is unavailable, status: {}",
                              fs_info_.name, FsStatus2Str(fs_info_.status));
    return Status::Internal("build meta system fail");
  }

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

  handle_manager_ = std::make_unique<HandleManager>(this);

  {
    if (FLAGS_vfs_use_fake_block_store) {
      block_store_ = std::make_unique<FakeBlockStore>(this, fs_info_.uuid);
    } else {
      block_store_ = std::make_unique<BlockStoreImpl>(this, fs_info_.uuid,
                                                      block_accesser_.get());
    }
    DINGOFS_RETURN_NOT_OK(block_store_->Start());
  }

  {
    flush_executor_ = std::make_unique<ExecutorImpl>(kFlushExecutorName,
                                                     FLAGS_vfs_flush_bg_thread);
    flush_executor_->Start();
  }

  {
    read_executor_ = std::make_unique<ExecutorImpl>(
        kReadExecutorName, FLAGS_vfs_read_executor_thread);
    read_executor_->Start();
  }

  {
    if (FLAGS_data_stream_page_use_pool) {
      page_allocator_ = std::make_shared<PagePool>();
    } else {
      page_allocator_ = std::make_shared<DefaultPageAllocator>();
    }

    auto ok =
        page_allocator_->Init(FLAGS_data_stream_page_size,
                              (FLAGS_data_stream_page_total_size_mb * kMiB) /
                                  FLAGS_data_stream_page_size);
    if (!ok) {
      LOG(ERROR) << "[vfs.hub] init page allocator failed.";
      return Status::Internal("init page allocator failed");
    }
  }

  {
    int64_t total_mb = FLAGS_vfs_read_buffer_total_mb;
    if (total_mb <= 0) {
      LOG(ERROR) << "[vfs.hub] invalid read buffer total mb option, total_mb: "
                 << total_mb;
      return Status::Internal("invalid read buffer total mb option");
    }

    read_buffer_manager_ =
        std::make_unique<ReadBufferManager>(total_mb * 1024 * 1024);
  }

  {
    file_suffix_watcher_ =
        std::make_unique<FileSuffixWatcher>(FLAGS_vfs_data_writeback_suffix);
  }

  {
    if (block_store_->EnableCache()) {
      prefetch_manager_ = PrefetchManager::New(this);
      auto status = prefetch_manager_->Start(FLAGS_vfs_prefetch_threads);
      if (!status.ok()) {
        LOG(ERROR) << "[vfs.hub] prefetch manager start failed.";
        return status;
      }
    } else {
      LOG(INFO)
          << "[vfs.hub] block cache not enable, skip prefetch manager start.";
    }
  }

  {
    warmup_manager_ = WarmupManager::New(this);
    auto ok = warmup_manager_->Start(FLAGS_vfs_warmup_threads);
    if (!ok.ok()) {
      LOG(ERROR) << "[vfs.hub] warmup manager start failed.";
      return ok;
    }
  }

  started_.store(true, std::memory_order_relaxed);
  return Status::OK();
}

Status VFSHubImpl::Stop(bool upgrade) {
  if (!started_.load(std::memory_order_relaxed)) {
    return Status::OK();
  }

  LOG(INFO) << fmt::format("[vfs.hub] vfs hub stopping, upgrade({}).", upgrade);

  if (handle_manager_ != nullptr) {
    handle_manager_->Shutdown();
    handle_manager_.reset();
  }

  if (read_executor_ != nullptr) {
    read_executor_->Stop();
    read_executor_.reset();
  }

  if (flush_executor_ != nullptr) {
    flush_executor_->Stop();
    flush_executor_.reset();
  }

  if (warmup_manager_ != nullptr) {
    warmup_manager_->Stop();
    warmup_manager_.reset();
  }

  if (prefetch_manager_ != nullptr) {
    prefetch_manager_->Stop();
    prefetch_manager_.reset();
  }

  if (block_store_ != nullptr) {
    block_store_->Shutdown();
    block_store_.reset();
  }

  if (meta_system_ != nullptr) {
    meta_system_->UnInit(upgrade);
    meta_system_.reset();
  }

  if (trace_manager_ != nullptr) {
    trace_manager_->Stop();
    trace_manager_.reset();
  }

  started_.store(false, std::memory_order_relaxed);

  return Status::OK();
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs
