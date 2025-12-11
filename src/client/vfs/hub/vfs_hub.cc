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

#include "cache/tiercache/tier_block_cache.h"
#include "client/common/const.h"
#include "client/vfs/background/periodic_flush_manager.h"
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
#include "common/blockaccess/block_accesser.h"
#include "common/blockaccess/rados/rados_common.h"
#include "common/options/cache.h"
#include "common/options/client.h"
#include "common/status.h"
#include "common/trace/log_trace_exporter.h"
#include "common/trace/noop_tracer.h"
#include "common/trace/tracer.h"
#include "fmt/format.h"
#include "gflags/gflags.h"
#include "glog/logging.h"
#include "utils/executor/thread/executor_impl.h"

namespace dingofs {
namespace client {
namespace vfs {

static const std::string kFlushExecutorName = "vfs_flush";
static const std::string kReadExecutorName = "vfs_read";

Status VFSHubImpl::Start(const VFSConfig& vfs_conf, const VFSOption& vfs_option,
                         bool upgrade) {
  CHECK(started_.load(std::memory_order_relaxed) == false)
      << "unexpected start";

  LOG(INFO) << fmt::format("[vfs.hub] vfs hub starting, upgrade({}).", upgrade);

  vfs_option_ = vfs_option;

  MetaSystemUPtr rela_meta_system;
  if (vfs_conf.fs_type == "vfs_memory") {
    rela_meta_system = std::make_unique<memory::MemoryMetaSystem>();

  } else if (vfs_conf.fs_type == "vfs_local") {
    rela_meta_system =
        std::make_unique<local::LocalMetaSystem>("", vfs_conf.fs_name);

  } else if (vfs_conf.fs_type == "vfs_v2" || vfs_conf.fs_type == "vfs_mds") {
    rela_meta_system = v2::MDSMetaSystem::Build(vfs_conf.fs_name,
                                                vfs_conf.mds_addrs, client_id_);
  }
  CHECK(rela_meta_system != nullptr) << "build meta system fail";

  if (FLAGS_trace_logging) {
    tracer_ = std::make_unique<Tracer>(
        std::make_unique<LogTraceExporter>(kVFSMoudule, FLAGS_log_dir));
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
  if (fs_info_.storage_info.store_type == StoreType::kS3) {
    auto s3_info = fs_info_.storage_info.s3_info;
    vfs_option_.block_access_opt.type = blockaccess::AccesserType::kS3;
    vfs_option_.block_access_opt.s3_options.s3_info =
        blockaccess::S3Info{.ak = s3_info.ak,
                            .sk = s3_info.sk,
                            .endpoint = s3_info.endpoint,
                            .bucket_name = s3_info.bucket_name};

  } else if (fs_info_.storage_info.store_type == StoreType::kRados) {
    auto rados_info = fs_info_.storage_info.rados_info;
    vfs_option_.block_access_opt.type = blockaccess::AccesserType::kRados;
    vfs_option_.block_access_opt.rados_options =
        blockaccess::RadosOptions{.mon_host = rados_info.mon_host,
                                  .user_name = rados_info.user_name,
                                  .key = rados_info.key,
                                  .pool_name = rados_info.pool_name,
                                  .cluster_name = rados_info.cluster_name};
  }

  block_accesser_ = blockaccess::NewBlockAccesser(vfs_option_.block_access_opt);
  DINGOFS_RETURN_NOT_OK(block_accesser_->Init());

  handle_manager_ = std::make_unique<HandleManager>(this);

  {
    // related to block cache
    cache::FLAGS_cache_dir_uuid = fs_info_.uuid;
    block_cache_ =
        std::make_unique<cache::TierBlockCache>(block_accesser_.get());
    DINGOFS_RETURN_NOT_OK(block_cache_->Start());
  }

  {
    flush_executor_ = std::make_unique<ExecutorImpl>(
        kFlushExecutorName, FLAGS_client_vfs_flush_bg_thread);
    flush_executor_->Start();
  }

  {
    read_executor_ = std::make_unique<ExecutorImpl>(
        kReadExecutorName, FLAGS_client_vfs_read_executor_thread);
    read_executor_->Start();
  }

  {
    priodic_flush_manager_ = std::make_unique<PeriodicFlushManager>(this);
    priodic_flush_manager_->Start();
  }

  {
    auto o = vfs_option_.page_option;
    if (o.use_pool) {
      page_allocator_ = std::make_shared<PagePool>();
    } else {
      page_allocator_ = std::make_shared<DefaultPageAllocator>();
    }

    auto ok = page_allocator_->Init(o.page_size, o.total_size / o.page_size);
    if (!ok) {
      LOG(ERROR) << "[vfs.hub] init page allocator failed.";
      return Status::Internal("init page allocator failed");
    }
  }

  {
    int64_t total_mb = FLAGS_client_vfs_read_buffer_total_mb;
    if (total_mb <= 0) {
      LOG(ERROR) << "[vfs.hub] invalid read buffer total mb option, total_mb: "
                 << total_mb;
      return Status::Internal("invalid read buffer total mb option");
    }

    read_buffer_manager_ =
        std::make_unique<ReadBufferManager>(total_mb * 1024 * 1024);
  }

  {
    file_suffix_watcher_ = std::make_unique<FileSuffixWatcher>(
        vfs_option_.data_option.writeback_suffix);
  }

  {
    if (block_cache_->EnableCache()) {
      prefetch_manager_ = PrefetchManager::New(this);
      auto status = prefetch_manager_->Start(FLAGS_client_vfs_prefetch_threads);
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
    auto ok = warmup_manager_->Start(FLAGS_client_vfs_warmup_threads);
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

  if (handle_manager_ != nullptr) handle_manager_->Shutdown();

  // shutdown before block cache
  if (priodic_flush_manager_ != nullptr) priodic_flush_manager_->Stop();
  if (read_executor_ != nullptr) read_executor_->Stop();
  if (flush_executor_ != nullptr) flush_executor_->Stop();
  if (warmup_manager_ != nullptr) warmup_manager_->Stop();
  if (prefetch_manager_ != nullptr) prefetch_manager_->Stop();
  if (block_cache_ != nullptr) block_cache_->Shutdown();
  if (meta_system_ != nullptr) meta_system_->UnInit(upgrade);

  started_.store(false, std::memory_order_relaxed);

  return Status::OK();
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs
