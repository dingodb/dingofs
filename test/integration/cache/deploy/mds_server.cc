/*
 * Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
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
 * Created Date: 2026-06-22
 * Author: AI
 */

#include "test/integration/cache/deploy/mds_server.h"

#include <brpc/channel.h>
#include <brpc/controller.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include <filesystem>
#include <fstream>

#include "dingofs/error.pb.h"
#include "dingofs/mds.pb.h"

namespace dingofs {
namespace cache {
namespace integration {

DEFINE_string(dingo_mds_bin, "",
              "path to the dingo-mds binary; empty resolves to the binary next "
              "to the test executable");

static constexpr uint64_t kBlockSize = 4ull * 1024 * 1024;   // 4MiB
static constexpr uint64_t kChunkSize = 64ull * 1024 * 1024;  // 64MiB

static std::string MdsBinary() {
  if (!FLAGS_dingo_mds_bin.empty()) return FLAGS_dingo_mds_bin;
  return ExeDir() + "/../dingo-mds";
}

static Status RenderConf(const std::string& path, int port,
                         const std::string& log_dir) {
  std::ofstream out(path);
  if (!out) {
    return Status::Internal("open mds conf for writing failed: " + path);
  }
  out << "--mds_cluster_id=1\n"
      << "--mds_storage_engine=dummy\n"
      << "--storage_url=\n"
      << "--mds_server_id=1\n"
      << "--mds_server_host=127.0.0.1\n"
      << "--mds_server_listen_host=127.0.0.1\n"
      << "--mds_server_port=" << port << "\n"
      << "--log_dir=" << log_dir << "\n"
      << "--log_level=INFO\n"
      << "--log_v=0\n"
      << "--rpc_channel_timeout_ms=30000\n"
      << "--rpc_channel_connect_timeout_ms=500\n"
      << "--rpc_time_out_ms=30000\n"
      << "--stale_period_us=500\n"
      << "--enable_rocksdb_perf_metric=true\n";
  return out.good() ? Status::OK()
                    : Status::Internal("write mds conf failed: " + path);
}

Status MdsServer::Start(const std::string& workdir) {
  workdir_ = workdir;
  port_ = PickFreePort();

  namespace fs = std::filesystem;
  std::error_code ec;
  fs::create_directories(workdir_ + "/log", ec);

  const std::string conf = workdir_ + "/mds.conf";
  auto status = RenderConf(conf, port_, workdir_ + "/log");
  if (!status.ok()) return status;

  const std::string bin = MdsBinary();
  if (!fs::exists(bin, ec)) {
    return Status::Internal("dingo-mds binary not found: " + bin +
                            " (pass --dingo_mds_bin)");
  }

  std::vector<std::string> argv = {bin, "--conf=" + conf, "--daemonize=false"};
  if (!proc_.Start(argv, workdir_ + "/mds.out")) {
    return Status::Internal("spawn dingo-mds failed");
  }

  if (!WaitPort("127.0.0.1", port_)) {
    return Status::Internal("dingo-mds did not open port " +
                            std::to_string(port_));
  }
  return Status::OK();
}

Status MdsServer::CreateLocalFs(const std::string& fs_name,
                                const std::string& storage_path,
                                uint32_t* fs_id) {
  namespace fs = std::filesystem;
  std::error_code ec;
  fs::create_directories(storage_path, ec);

  brpc::Channel channel;
  brpc::ChannelOptions opt;
  opt.timeout_ms = 10000;
  opt.max_retry = 3;
  if (channel.Init(Addr().c_str(), &opt) != 0) {
    return Status::Internal("init channel to mds failed: " + Addr());
  }

  pb::mds::MDSService_Stub stub(&channel);
  pb::mds::CreateFsRequest request;
  pb::mds::CreateFsResponse response;

  request.set_fs_name(fs_name);
  request.set_block_size(kBlockSize);
  request.set_chunk_size(kChunkSize);
  request.set_owner("test");
  request.set_capacity(1ull * 1024 * 1024 * 1024);
  request.set_partition_type(pb::mds::PartitionType::MONOLITHIC_PARTITION);
  request.set_fs_type(pb::mds::FsType::LOCALFILE);
  request.mutable_fs_extra()->mutable_file_info()->set_path(storage_path);

  brpc::Controller cntl;
  cntl.set_timeout_ms(10000);
  stub.CreateFs(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    return Status::Internal("CreateFs rpc failed: " + cntl.ErrorText());
  }
  if (response.error().errcode() != pb::error::Errno::OK) {
    return Status::Internal("CreateFs error: " +
                            response.error().ShortDebugString());
  }

  *fs_id = response.fs_info().fs_id();
  LOG(INFO) << "created LOCALFILE fs '" << fs_name << "' fs_id=" << *fs_id
            << " path=" << storage_path;
  return Status::OK();
}

}  // namespace integration
}  // namespace cache
}  // namespace dingofs
