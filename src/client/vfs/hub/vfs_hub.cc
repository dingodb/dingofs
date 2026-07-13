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

#include "cache/infiniband/memory.h"
#include "client/vfs/blockstore/block_store_impl.h"
#include "client/vfs/blockstore/fake_block_store.h"
#include "client/vfs/common/helper.h"
#include "client/vfs/compaction/compactor_impl.h"
#include "client/vfs/components/prefetch_manager.h"
#include "client/vfs/components/warmup_manager.h"
#include "client/vfs/data/writer_table.h"
#include "client/vfs/metasystem/local/metasystem.h"
#include "client/vfs/metasystem/mds/metasystem.h"
#include "client/vfs/metasystem/memory/metasystem.h"
#include "client/vfs/metasystem/meta_wrapper.h"
#include "client/vfs/vfs.h"
#include "client/vfs/vfs_meta.h"
#include "common/blockaccess/accesser_common.h"
#include "common/blockaccess/block_accesser.h"
#include "common/blockaccess/prefix_block_accesser.h"
#include "common/blockaccess/rados/rados_common.h"
#include "common/directory.h"
#include "common/options/cache.h"
#include "common/options/client.h"
#include "common/options/common.h"
#include "common/status.h"
#include "common/version.h"
#include "utils/executor/thread/executor_impl.h"

namespace dingofs {
namespace client {
namespace vfs {

static const std::string kFlushExecutorName = "vfs_flush";
static const std::string kReadExecutorName = "vfs_read";
static const std::string kReadCleanupExecutorName = "vfs_read_cleanup";
static const std::string kWriteBackgroundExecutorName = "vfs_write_bg";
static const std::string kCBExecutorName = "vfs_callback";

static MetaSystemUPtr BuildMetaSystem(const VFSConfig& vfs_conf,
                                      const ClientId& client_id,
                                      TraceManager& trace_manager,
                                      Compactor& compactor) {
  if (vfs_conf.metasystem_type == MetaSystemType::MEMORY) {
    return std::make_unique<memory::MemoryMetaSystem>();

  } else if (vfs_conf.metasystem_type == MetaSystemType::LOCAL) {
    return std::make_unique<local::LocalMetaSystem>(
        GetDefaultDir(kMetaDir), vfs_conf.fs_name, vfs_conf.storage_info);

  } else if (vfs_conf.metasystem_type == MetaSystemType::MDS) {
    return meta::MDSMetaSystem::Build(vfs_conf.fs_name, vfs_conf.mds_addrs,
                                      client_id, trace_manager, compactor);
  }

  return nullptr;
}

VFSHubImpl::VFSHubImpl(const VFSConfig& vfs_conf, ClientId client_id)
    : client_id_(client_id), vfs_conf_(vfs_conf) {}

WriterTable* VFSHubImpl::GetWriterTable() {
  CHECK_NOTNULL(writer_table_);
  return writer_table_.get();
}

VFSHubImpl::~VFSHubImpl() {
  // Quiesce before tearing members down. Stop() is idempotent, so this is a
  // no-op after a normal shutdown that already stopped the hub; but it is the
  // only thing that stops live components when Start() failed half-way and the
  // external Stop() path was skipped -- otherwise e.g. ~BatchProcessor would
  // pthread_cond_destroy a condition variable its worker is still waiting on
  // and hang the process forever.
  //
  // skip_unmount=false: the only case where this actually runs (rather than
  // no-op) is a dying process whose external Stop() never ran -- it should
  // deregister its own client_id from the MDS (UnmountFs is per-client_id, it
  // never touches a peer's mount). A successful handover already ran an
  // explicit Stop(/*skip_unmount=*/true), so this is a no-op there and does not
  // override it.
  Stop(/*skip_unmount=*/false);

  if (handle_manager_ != nullptr) {
    handle_manager_.reset();
  }

  if (writer_table_ != nullptr) {
    writer_table_.reset();
  }

  if (read_executor_ != nullptr) {
    read_executor_.reset();
  }

  if (read_cleanup_executor_ != nullptr) {
    read_cleanup_executor_.reset();
  }

  if (write_background_executor_ != nullptr) {
    write_background_executor_.reset();
  }

  if (flush_executor_ != nullptr) {
    flush_executor_.reset();
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

  if (logclean_manager_ != nullptr) {
    logclean_manager_.reset();
  }
}

Status VFSHubImpl::Start(bool skip_mount) {
  CHECK(started_.load(std::memory_order_relaxed) == false)
      << "unexpected start";

  // Arm teardown before creating any component: if Start() fails half-way, the
  // destructor's Stop() must still quiesce whatever came up.
  stopped_.store(false, std::memory_order_relaxed);

  LOG(INFO) << fmt::format("[vfs.hub] vfs hub starting, skip_mount({}).",
                           skip_mount);

  // trace manager
  trace_manager_ = std::make_unique<TraceManager>();
  if (FLAGS_enable_trace) {
    if (!trace_manager_->Init()) {
      return Status::Internal("init trace manager fail");
    }
  }

  {
    compactor_ = std::make_unique<CompactorImpl>(this);
    DINGOFS_RETURN_NOT_OK(compactor_->Start());
  }

  // meta system
  {
    auto meta =
        BuildMetaSystem(vfs_conf_, client_id_, *trace_manager_, *compactor_);
    if (meta == nullptr) {
      return Status::Internal("build meta system fail");
    }

    meta_wrapper_ = std::make_unique<MetaWrapper>(std::move(meta));
    DINGOFS_RETURN_NOT_OK(meta_wrapper_->Init(skip_mount));
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

  // uid/gid mapper + passwd watcher
  {
    // salt = fs uuid; enabled comes from fs_info and is immutable after fs
    // creation, so there is no runtime toggle. Init() primes the snapshot and
    // starts the /etc passwd/group inotify watch.
    uid_gid_mapper_ = std::make_unique<UidGidMapper>(
        fs_info_.enable_uid_gid_map, fs_info_.uuid,
        std::make_unique<LibcPasswdSource>());
    uid_gid_mapper_->Init();
  }

  // block accesser
  {
    // 1) decide backend type from fs_info and set per-backend connection
    //    info FIRST. The backend-specific gflag-fill below depends on
    //    `type` being correct.
    if (fs_info_.storage_info.store_type == StoreType::kS3) {
      auto s3_info = fs_info_.storage_info.s3_info;
      blockaccess_options_.type = blockaccess::AccesserType::kS3;
      blockaccess_options_.s3_options.s3_info =
          blockaccess::S3Info{.ak = s3_info.ak,
                              .sk = s3_info.sk,
                              .endpoint = s3_info.endpoint,
                              .bucket_name = s3_info.bucket_name};
      blockaccess::FillAwsSdkConfigFromGFlags(
          &blockaccess_options_.s3_options.aws_sdk_config);
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

    // 2) Throttle options are backend-agnostic.
    blockaccess::FillThrottleOptionsFromGFlags(
        &blockaccess_options_.throttle_options);

    block_accesser_ = blockaccess::NewPrefixBlockAccesser(fs_info_.name,
                                                          blockaccess_options_);
    DINGOFS_RETURN_NOT_OK(block_accesser_->Init());
  }

  // writer table — must start before handle_manager since NewHandle calls
  // GetWriterTable()->AcquireWriter for writable opens.
  {
    writer_table_ = std::make_unique<WriterTable>(this);
    CHECK(writer_table_ != nullptr) << "writer table is nullptr.";
    DINGOFS_RETURN_NOT_OK(writer_table_->Start());
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
    read_cleanup_executor_ = std::make_unique<ExecutorImpl>(
        kReadCleanupExecutorName, FLAGS_vfs_read_cleanup_executor_thread);
    if (!read_cleanup_executor_->Start()) {
      return Status::Internal("read cleanup executor start fail");
    }
  }

  {
    write_background_executor_ = std::make_unique<ExecutorImpl>(
        kWriteBackgroundExecutorName,
        FLAGS_vfs_write_background_executor_thread);
    if (!write_background_executor_->Start()) {
      return Status::Internal("write background executor start fail");
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

  write_buffer_manager_ = std::make_unique<WriteMemPool>(
      FLAGS_vfs_write_buffer_total_mb * 1024 * 1024,
      FLAGS_vfs_write_buffer_page_size);

  // read mempool (the read-path buffer accountant; replaces ReadBufferManager)
  {
    int64_t total_bytes = FLAGS_vfs_read_buffer_total_mb * 1024 * 1024;
    if (total_bytes <= 0) {
      return Status::Internal("invalid vfs_read_buffer_total_mb");
    }

    read_mem_pool_ =
        std::make_unique<ReadMemPool>(static_cast<size_t>(total_bytes));
    if (!read_mem_pool_->Valid()) {
      return Status::Internal("read mem pool arena create failed");
    }
    read_mem_pool_vars_ =
        std::make_unique<ReadMemPoolVars>(read_mem_pool_.get(), "vfs");
  }

  // register memory for rdma
  {
    if (cache::FLAGS_use_rdma) {
      CHECK_NOTNULL(read_mem_pool_->BaseAddr());
      CHECK_NOTNULL(write_buffer_manager_->BaseAddr());

      auto status = cache::infiniband::RegisterMemoryForRDMA(
          cache::FLAGS_cache_rdma_device, read_mem_pool_->BaseAddr(),
          read_mem_pool_->TotalSize());
      if (!status.ok()) {
        return status;
      }

      status = cache::infiniband::RegisterMemoryForRDMA(
          cache::FLAGS_cache_rdma_device, write_buffer_manager_->BaseAddr(),
          write_buffer_manager_->TotalSize());
      if (!status.ok()) {
        return status;
      }
    }
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
      LOG(INFO) << fmt::format(
          "[vfs.hub] block cache not enable, skip prefetch manager start.");
    }
  }

  // warmup manager
  {
    warmup_manager_ = WarmupManager::New(this);
    CHECK(warmup_manager_ != nullptr) << "warmup manager is nullptr.";
    DINGOFS_RETURN_NOT_OK(warmup_manager_->Start(FLAGS_vfs_warmup_threads));
    // Inject the manager into the meta system so directory-driven warmup
    // can fire from Open. Wired here (post-construction) to avoid a cyclic
    // dependency between MetaWrapper and WarmupManager during build.
    meta_wrapper_->SetWarmupManager(warmup_manager_.get());
  }

  // log clean manager
  {
    logclean_manager_ =
        std::make_unique<utils::LogCleanManager>(::FLAGS_log_dir);
    CHECK(logclean_manager_ != nullptr) << "log clean manager is nullptr.";
    DINGOFS_RETURN_NOT_OK(logclean_manager_->Start());
  }

  ExposeDingoVersion();

  started_.store(true, std::memory_order_relaxed);

  return Status::OK();
}

// NOTE: the stop sequence is important, please do not change it lightly.
Status VFSHubImpl::Stop(bool skip_unmount) {
  // Teardown-once gate. The first caller runs the full quiescence; later
  // callers (the destructor after a normal Stop, or a second handover Stop)
  // no-op. Gated on stopped_ rather than started_ so a half-started hub (e.g.
  // Start failed after components came up because the brpc dummy server could
  // not bind) still tears its live components down instead of leaking them into
  // a destructor that would hang (~BatchProcessor destroying an in-use
  // condition variable). NOTE: assumes Stop() calls are sequential (they are on
  // the teardown path); concurrent Stop() would need a wait-for-completion
  // guard, not exchange.
  if (stopped_.exchange(true)) {
    return Status::OK();
  }

  LOG(INFO) << fmt::format("[vfs.hub] stopping vfs hub, skip_unmount({}).",
                           skip_unmount);

  if (uid_gid_mapper_ != nullptr) {
    uid_gid_mapper_->StopWatching();
  }

  if (compactor_ != nullptr) {
    compactor_->Stop();
  }

  if (handle_manager_ != nullptr) {
    handle_manager_->Stop();
  }

  // HandleManager::Stop releases handle-owned reader/writer resources while
  // preserving handle identities for handover Dump. Drain residual dirty data
  // and stop the writer table before tearing down executors/meta/block-store.
  if (writer_table_ != nullptr) {
    (void)writer_table_->FlushAll();
    writer_table_->Stop();
  }

  if (read_executor_ != nullptr) {
    read_executor_->Stop();
  }

  // Writer-side background tasks may call meta_system/block_store or enqueue
  // flush work, so drain them while those dependencies are still alive.
  if (write_background_executor_ != nullptr) {
    write_background_executor_->Stop();
  }

  // DoFlush is the last writer-side producer of BlockStore uploads. Drain it
  // before shutting BlockStore down. Upload completion no longer schedules
  // work back to FlushExecutor, so this dependency is one-way.
  if (flush_executor_ != nullptr) {
    flush_executor_->Stop();
  }

  if (warmup_manager_ != nullptr) {
    // Detach from meta system before stopping so any in-flight Open hook
    // observes nullptr instead of a half-stopped manager.
    if (meta_wrapper_ != nullptr) {
      meta_wrapper_->SetWarmupManager(nullptr);
    }
    warmup_manager_->Stop();
  }

  if (prefetch_manager_ != nullptr) {
    prefetch_manager_->Stop();
  }

  // Block cache read/prefetch completions run in cache-owned bthreads and may
  // schedule read cleanup work. Keep read_cleanup_executor_ alive until
  // block_store_->Shutdown() has joined those bthreads.
  if (block_store_ != nullptr) {
    block_store_->Shutdown();
  }

  // BlockStore::Shutdown has invoked every accepted upload callback. Drain
  // CBExecutor while MetaSystem is still available to completion callbacks.
  if (cb_executor_ != nullptr) {
    cb_executor_->Stop();
  }

  // read_cleanup_executor_ is the consumer of read completions (it erases
  // finished read requests); stop it only after block_store_ has joined every
  // read bthread, otherwise a straggler completion submits to a dead executor.
  if (read_cleanup_executor_ != nullptr) {
    read_cleanup_executor_->Stop();
  }

  // meta_wrapper_ can stop after block_store_: the metasystem flushes to MDS
  // via RPC and never pushes data through block_store_, and its only background
  // producer (write_background_executor_'s slice_id pre-allocation) was already
  // drained above. Block-store completions are drained by CBExecutor above.
  if (meta_wrapper_ != nullptr) {
    meta_wrapper_->Stop(skip_unmount);
  }

  if (trace_manager_ != nullptr) {
    if (FLAGS_enable_trace) trace_manager_->Stop();
  }

  if (logclean_manager_ != nullptr) {
    logclean_manager_->Stop();
  }

  LOG(INFO) << fmt::format("[vfs.hub] stopped vfs hub, skip_unmount({}).",
                           skip_unmount);

  // Mark not-serving only after every component is stopped. GetFsInfo() /
  // GetBlockAccesserOptions() CHECK(started_), and a draining background task
  // (e.g. an in-flight compaction the compactor is joining) may still call them
  // while the components are being torn down. Clearing started_ at the top of
  // Stop() would CHECK-fail (SIGABRT) such a late caller mid-teardown.
  started_.store(false, std::memory_order_relaxed);

  return Status::OK();
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs
