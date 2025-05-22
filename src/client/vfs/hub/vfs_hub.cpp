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

#include "cache/blockcache/block_cache.h"
#include "client/common/config.h"
#include "client/vfs/meta/dummy/dummy_filesystem.h"
#include "client/vfs/meta/v2/filesystem.h"
#include "client/vfs/vfs.h"
#include "client/vfs/vfs_meta.h"
#include "common/status.h"
#include "blockaccess/block_accesser.h"
#include "utils/configuration.h"

namespace dingofs {
namespace client {
namespace vfs {

Status VFSHubImpl::Start(const VFSConfig& vfs_conf,
                         const common::ClientOption& client_option) {
  CHECK(started_.load(std::memory_order_relaxed) == false)
      << "unexpected start";

  client_option_ = client_option;

  utils::Configuration conf;
  conf.SetConfigPath(vfs_conf.config_path);
  if (!conf.LoadConfig()) {
    LOG(ERROR) << "load config fail, confPath = " << vfs_conf.config_path;
    return Status::Internal("load config fail");
  }

  if (vfs_conf.fs_type == "vfs_dummy") {
    LOG(INFO) << "use dummy file system.";
    meta_system_ = std::make_unique<dummy::DummyFileSystem>();

  } else if (vfs_conf.fs_type == "vfs_v2") {
    LOG(INFO) << "use mdsv2 file system.";
    std::string mds_addr;
    conf.GetValueFatalIfFail("mds.addr", &mds_addr);
    LOG(INFO) << fmt::format("mds addr: {}.", mds_addr);

    meta_system_ = v2::MDSV2FileSystem::Build(vfs_conf.fs_name, mds_addr,
                                              vfs_conf.mount_point);
  } else {
    LOG(INFO) << fmt::format("not unknown file system {}.", vfs_conf.fs_type);
    return Status::Internal("not unknown file system");
  }

  if (meta_system_ == nullptr) {
    return Status::Internal("build meta system fail");
  }

  DINGOFS_RETURN_NOT_OK(meta_system_->Init());

  DINGOFS_RETURN_NOT_OK(meta_system_->GetFsInfo(&fs_info_));

  block_accesser_ = std::make_unique<blockaccess::BlockAccesserImpl>(
      client_option_.block_access_opt);
  DINGOFS_RETURN_NOT_OK(block_accesser_->Init());

  handle_manager_ = std::make_unique<HandleManager>();

  {
    // related to block cache
    auto block_cache_option = client_option_.block_cache_option;

    std::string uuid = absl::StrFormat("%d-%s", fs_info_.id, fs_info_.name);
    common::RewriteCacheDir(&block_cache_option, uuid);

    block_cache_ = std::make_unique<cache::blockcache::BlockCacheImpl>(
        block_cache_option, block_accesser_.get());

    DINGOFS_RETURN_NOT_OK(block_cache_->Init());
  }

  started_.store(true, std::memory_order_relaxed);
  return Status::OK();
}

Status VFSHubImpl::Stop() {
  if (!started_.load(std::memory_order_relaxed)) {
    return Status::OK();
  }

  started_.store(false, std::memory_order_relaxed);

  meta_system_->UnInit();

  return Status::OK();
}

MetaSystem* VFSHubImpl::GetMetaSystem() {
  CHECK_NOTNULL(meta_system_);
  return meta_system_.get();
}

HandleManager* VFSHubImpl::GetHandleManager() {
  CHECK_NOTNULL(handle_manager_);
  return handle_manager_.get();
}

cache::blockcache::BlockCache* VFSHubImpl::GetBlockCache() {
  CHECK_NOTNULL(handle_manager_);
  return block_cache_.get();
}

blockaccess::BlockAccesser* VFSHubImpl::GetBlockAccesser() {
  CHECK_NOTNULL(block_accesser_);
  return block_accesser_.get();
}

FsInfo VFSHubImpl::GetFsInfo() {
  CHECK(started_.load(std::memory_order_relaxed)) << "not started";
  return fs_info_;
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs