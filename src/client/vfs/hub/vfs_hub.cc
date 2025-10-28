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
#include <memory>

#include "cache/tiercache/tier_block_cache.h"
#include "client/common/const.h"
#include "client/vfs/background/periodic_flush_manager.h"
#include "client/vfs/common/helper.h"
#include "client/vfs/components/prefetch_manager.h"
#include "client/vfs/components/warmup_manager.h"
#include "client/vfs/metasystem/dummy/dummy_filesystem.h"
#include "client/vfs/metasystem/mds/filesystem.h"
#include "client/vfs/metasystem/meta_system.h"
#include "client/vfs/metasystem/meta_wrapper.h"
#include "client/vfs/vfs.h"
#include "client/vfs/vfs_meta.h"
#include "common/blockaccess/block_accesser.h"
#include "common/blockaccess/rados/rados_common.h"
#include "common/options/cache.h"
#include "common/options/client.h"
#include "common/options/trace.h"
#include "common/status.h"
#include "common/trace/log_trace_exporter.h"
#include "common/trace/noop_tracer.h"
#include "common/trace/tracer.h"
#include "fmt/format.h"
#include "gflags/gflags.h"
#include "glog/logging.h"
#include "utils/configuration.h"
#include "utils/executor/thread/executor_impl.h"

namespace dingofs {
namespace client {
namespace vfs {

Status VFSHubImpl::Start(const VFSConfig& vfs_conf,
                         const VFSOption& vfs_option) {
  CHECK(started_.load(std::memory_order_relaxed) == false)
      << "unexpected start";

  vfs_option_ = vfs_option;

  utils::Configuration conf;
  conf.SetConfigPath(vfs_conf.config_path);
  if (!conf.LoadConfig()) {
    LOG(ERROR) << "load config fail, confPath = " << vfs_conf.config_path;
    return Status::Internal("load config fail");
  }

  std::unique_ptr<MetaSystem> rela_meta_system;
  if (vfs_conf.fs_type == "vfs_dummy") {
    LOG(INFO) << "use dummy file system.";
    rela_meta_system = std::make_unique<dummy::DummyFileSystem>();

  } else if (vfs_conf.fs_type == "vfs_v2") {
    LOG(INFO) << "use mds file system.";
    std::string mds_addrs;
    conf.GetValueFatalIfFail("mds.addr", &mds_addrs);
    LOG(INFO) << fmt::format("mds addr: {}.", mds_addrs);

    rela_meta_system = v2::MDSFileSystem::Build(vfs_conf.fs_name, mds_addrs,
                                                vfs_conf.mount_point,
                                                vfs_option.dummy_server_port);
  } else {
    LOG(INFO) << fmt::format("not unknown file system {}.", vfs_conf.fs_type);
    return Status::Internal("not unknown file system");
  }

  if (rela_meta_system == nullptr) {
    return Status::Internal("build meta system fail");
  }

  if (FLAGS_trace_logging) {
    tracer_ = std::make_unique<Tracer>(
        std::make_unique<LogTraceExporter>(kVFSMoudule, FLAGS_log_dir));
  } else {
    tracer_ = std::make_unique<NoopTracer>();
  }

  auto span = tracer_->StartSpan(kVFSMoudule, "start");

  meta_system_ = std::make_unique<MetaWrapper>(std::move(rela_meta_system));

  DINGOFS_RETURN_NOT_OK(meta_system_->Init());

  DINGOFS_RETURN_NOT_OK(meta_system_->GetFsInfo(span->GetContext(), &fs_info_));

  LOG(INFO) << fmt::format("vfs_fs_info: {}", FsInfo2Str(fs_info_));
  if (fs_info_.status != FsStatus::kNormal) {
    LOG(ERROR) << fmt::format("fs {} is unavailable, status: {}", fs_info_.name,
                              FsStatus2Str(fs_info_.status));
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
    flush_executor_ =
        std::make_unique<ExecutorImpl>(FLAGS_client_vfs_flush_bg_thread);
    flush_executor_->Start();
  }

  {
    read_executor_ =
        std::make_unique<ExecutorImpl>(FLAGS_client_vfs_read_executor_thread);
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
      LOG(ERROR) << "Init page allocator failed.";
      return Status::Internal("Init page allocator failed");
    }
  }

  {
    file_suffix_watcher_ = std::make_unique<FileSuffixWatcher>(
        vfs_option_.data_option.writeback_suffix);
  }

  {
    prefetch_manager_ = std::make_unique<PrefecthManager>(
        this, fs_info_.id, fs_info_.chunk_size, fs_info_.block_size);
    auto ok = prefetch_manager_->Start();
    if (!ok.ok()) {
      LOG(ERROR) << "prefetch manager start failed";
      return ok;
    }
  }

  {
    warmup_manager_ = std::make_unique<WarmupManagerImpl>(this);
    auto ok = warmup_manager_->Start();
    if (!ok.ok()) {
      LOG(ERROR) << "warmup manager start failed";
      return ok;
    }
  }

  started_.store(true, std::memory_order_relaxed);
  return Status::OK();
}

Status VFSHubImpl::Stop() {
  if (!started_.load(std::memory_order_relaxed)) {
    return Status::OK();
  }

  handle_manager_->Shutdown();

  // shutdown before block cache
  priodic_flush_manager_->Stop();
  read_executor_->Stop();
  flush_executor_->Stop();
  warmup_manager_->Stop();
  prefetch_manager_->Stop();
  block_cache_->Shutdown();
  meta_system_->UnInit();

  started_.store(false, std::memory_order_relaxed);

  return Status::OK();
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs
