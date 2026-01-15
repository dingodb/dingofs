/*
 * Copyright (c) 2024 dingodb.com, Inc. All Rights Reserved
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

#include "client/fuse/fuse_server.h"

#include <brpc/reloadable_flags.h>
#include <sys/socket.h>

#include <chrono>
#include <cstdlib>

#include "absl/cleanup/cleanup.h"
#include "absl/strings/str_format.h"
#include "client/fuse/fuse_common.h"
#include "client/fuse/fuse_lowlevel_ops_func.h"
#include "client/fuse/fuse_passfd.h"
#include "client/fuse/fuse_upgrade_manager.h"
#include "common/const.h"
#include "common/helper.h"
#include "common/options/client.h"
#include "fmt/format.h"
#include "fuse_opt.h"
#include "glog/logging.h"
#include "utils/concurrent/concurrent.h"

using ::dingofs::utils::BufToHexString;

namespace dingofs {
namespace client {
namespace fuse {

USING_FLAG(fuse_fd_get_max_retries)
USING_FLAG(fuse_fd_get_retry_interval_ms)
USING_FLAG(fuse_check_alive_max_retries)
USING_FLAG(fuse_check_alive_retry_interval_ms)

// fuse mount options
DEFINE_string(fuse_mount_options, "default_permissions",
              "mount options for libfuse");
DEFINE_bool(fuse_use_single_thread, false, "use single thread for libfuse");
DEFINE_validator(fuse_use_single_thread, brpc::PassValidate);
DEFINE_bool(fuse_use_clone_fd, false, "use clone fd for libfuse");
DEFINE_validator(fuse_use_clone_fd, brpc::PassValidate);
DEFINE_uint32(fuse_max_threads, 64, "max threads for libfuse");
DEFINE_validator(fuse_max_threads, brpc::PassValidate);

DEFINE_string(socket_path, kDefaultSockDir,
              "path for store unix domain socket file");
DEFINE_validator(socket_path,
                 [](const char* /*name*/, const std::string& value) {
                   FLAGS_socket_path = dingofs::Helper::ExpandPath(value);
                   return true;
                 });

FuseServer::FuseServer() = default;

int FuseServer::Init(const std::string& program_name,
                     struct MountOption* mount_option) {
  mount_option_ = mount_option;
  program_name_ = program_name;

  AllocateFuseInitBuf();

  if (AddMountOptions() == 1) return 1;

  config_ = fuse_loop_cfg_create();
  CHECK(config_ != nullptr) << "fuse_loop_cfg_create fail.";

  CHECK(dingofs::Helper::CreateDirectory(FLAGS_socket_path))
      << "create directory" << FLAGS_socket_path << " fail";
  fd_comm_file_ =
      absl::StrFormat("%s/fd_comm_socket.%d", FLAGS_socket_path, getpid());

  return 0;
}

FuseServer::~FuseServer() {
  unlink(fd_comm_file_.c_str());

  FreeFuseInitBuf();
  fuse_opt_free_args(&args_);

  if (config_ != nullptr) {
    fuse_loop_cfg_destroy(config_);
  }

  auto fuse_state = FuseUpgradeManager::GetInstance().GetFuseState();
  if (fuse_state == FuseUpgradeState::kFuseUpgradeOld) {
    LOG(INFO) << "transfer dingo-client session to others.";
  }
}

// allocate memory for fuse init message
void FuseServer::AllocateFuseInitBuf() {
  // init message size = sizeof(struct fuse_in_header) + sizeof(struct
  // fuse_init_in),256 bytes is enough
  init_fbuf_.mem_size = 256;
  init_fbuf_.size = 0;
  init_fbuf_.mem = malloc(init_fbuf_.mem_size);
  memset(init_fbuf_.mem, 0, init_fbuf_.mem_size);
}

void FuseServer::FreeFuseInitBuf() {
  if (init_fbuf_.mem != nullptr) {
    free(init_fbuf_.mem);

    init_fbuf_.mem = nullptr;
    init_fbuf_.mem_size = 0;
    init_fbuf_.size = 0;
  }
}

int FuseServer::GetDevFd() const { return fuse_session_fd(session_); }

void FuseServer::Shutdown() {
  LOG(INFO) << "shutdown dingo-client";
  fuse_session_exit(session_);
}

void FuseServer::MarkThenShutdown() {
  LOG(INFO) << "mark dingo-client kFuseUpgradeOld";
  FuseUpgradeManager::GetInstance().UpdateFuseState(
      FuseUpgradeState::kFuseUpgradeOld);
  Shutdown();
}

void FuseServer::UdsServerFunc() {
  struct sockaddr_un addr;
  int server_fd, client_fd;
  socklen_t addrlen = sizeof(addr);

  // create uds server
  server_fd = socket(AF_UNIX, SOCK_STREAM, 0);
  if (server_fd == -1) {
    LOG(ERROR) << "uds server create failed, file: " << fd_comm_file_
               << ", error: " << std::strerror(errno);
    return;
  }
  auto defer_close = ::absl::MakeCleanup([&]() { close(server_fd); });
  // bind address
  memset(&addr, 0, sizeof(addr));
  addr.sun_family = AF_UNIX;
  strncpy(addr.sun_path, fd_comm_file_.c_str(), sizeof(addr.sun_path) - 1);
  unlink(fd_comm_file_.c_str());
  if (bind(server_fd, (struct sockaddr*)&addr, addrlen) == -1) {
    LOG(ERROR) << "uds server bind failed,, file: " << fd_comm_file_
               << ", error: " << std::strerror(errno);
    return;
  }
  // listening
  if (listen(server_fd, 1) == -1) {
    LOG(ERROR) << "uds server listen failed, file: " << fd_comm_file_
               << ", error: " << std::strerror(errno);
    return;
  }

  LOG(INFO) << "uds server listening on " << fd_comm_file_;
  while (true) {
    // accept uds client
    client_fd = accept(server_fd, (struct sockaddr*)&addr, &addrlen);
    if (client_fd == -1) {
      LOG(ERROR) << "uds server accept failed, error: " << std::strerror(errno);
      continue;
    }

    // process uds client request
    int fuse_fd = GetDevFd();
    int ret = SendFd(client_fd, fuse_fd, init_fbuf_.mem, init_fbuf_.size);
    if (ret == -1) {
      LOG(ERROR) << "send fuse fd failed, client_id: " << client_fd;
    } else {
      LOG(INFO) << "fuse fd send to client: " << client_fd
                << ", fd: " << fuse_fd << ", data size: " << init_fbuf_.size;
    }
    close(client_fd);
  }
}

void FuseServer::UdsServerStart() {
  if (is_running_.load()) {
    LOG(INFO) << "dingo-client uds server already started.";
    return;
  }

  uds_thread_ = utils::Thread(&FuseServer::UdsServerFunc, this);
  uds_thread_.detach();
  is_running_.store(true);

  LOG(INFO) << "dingo-client uds server started.";
}

int FuseServer::AddMountOptions() {
  CHECK(!program_name_.empty()) << "program_name_ should not be empty";
  if (fuse_opt_add_arg(&args_, program_name_.c_str()) != 0) return 1;

  //  Values shown in "df -T" and friends first column "Filesystem",DindoFS +
  //  filesystem name
  if (FuseAddOpts(&args_, (const char*)"subtype=dingofs") != 0) return 1;

  std::string arg_value =
      fmt::format("fsname=DingoFS:{}", mount_option_->fs_name);
  if (FuseAddOpts(&args_, arg_value.c_str()) != 0) return 1;

  if (FuseAddOpts(&args_, FLAGS_fuse_mount_options.c_str()) != 0) return 1;

  //  root user automatically enables the allow_other option
  if (getuid() == 0) {
    if (FuseAddOpts(&args_, "allow_other") != 0) return 1;
  }

  return 0;
}

int FuseServer::CreateSession(void* usedata) {
  // create fuse new session
  session_ = fuse_session_new(&args_, &kFuseOp, sizeof(kFuseOp), usedata);
  if (session_ == nullptr) return 1;

  // install fuse signal
  if (fuse_set_signal_handlers(session_) != 0) return 1;

  return 0;
}

void FuseServer::DestroySsesion() {
  LOG(INFO) << "destroy dingo-client session.";

  if (session_ != nullptr) {
    fuse_remove_signal_handlers(session_);
    fuse_session_destroy(session_);
  }
}

int FuseServer::SessionMount() {
  std::string mountpoint = mount_option_->mount_point;

  LOG(INFO) << fmt::format("Begin to mount fs {} to {}.",
                           mount_option_->fs_name, mountpoint);

  if (CanShutdownGracefully(mountpoint)) {
    bool is_shutdown = ShutdownGracefully(mountpoint);
    if (!is_shutdown) {
      LOG(ERROR) << "smooth upgrade failed, can't mount on: " << mountpoint;
      return 1;
    }
    LOG(INFO) << "old dingo-client is already shutdown";
    // new fuse processes
    FuseUpgradeManager::GetInstance().UpdateFuseState(
        FuseUpgradeState::kFuseUpgradeNew);

    // construct new mountpoint
    mountpoint = absl::StrFormat("/dev/fd/%d", fuse_fd_);
  }

  LOG(INFO) << "start mount on: " << mountpoint;
  if (fuse_session_mount(session_, mountpoint.c_str()) != 0) {
    LOG(ERROR) << "failed mount on: " << mountpoint;
    return 1;
  }

  return 0;
}

int FuseServer::SaveOpInitMsg() {
  // smooth upgrade do not save fuse init message
  // it's recv from old dingo-client
  auto fuse_state = FuseUpgradeManager::GetInstance().GetFuseState();
  if (fuse_state == FuseUpgradeState::kFuseUpgradeNew) return 0;

  struct fuse_buf fbuf = {
      .mem = nullptr,
  };

  int ret = fuse_session_receive_buf(session_, &fbuf);
  if (ret > 0) {
    LOG(INFO) << "recv dingo-client init message, size=" << fbuf.size;
    // save fuse init message
    CHECK(init_fbuf_.mem_size >= fbuf.size);
    init_fbuf_.size = fbuf.size;
    memcpy(init_fbuf_.mem, fbuf.mem, fbuf.size);
    fuse_session_process_buf(session_, &fbuf);

    return 0;
  }

  // here shouble call fuse_buf_free(&fbuf);
  // but function fuse_buf_free in libfuse is private, so we can not call it.
  // In special case, there may be a memory leak here,but rarely occurs and
  // can be tolerated. fuse_buf_free(&fbuf);
  fuse_session_reset(session_);

  return 1;
}

void FuseServer::ProcessInitMsg() {
  std::string msg =
      BufToHexString((unsigned char*)init_fbuf_.mem, init_fbuf_.size);
  LOG(INFO) << "dingo-client init data size: " << init_fbuf_.size
            << ", data: 0x" << msg;
  fuse_session_process_buf(session_, &init_fbuf_);
}

int FuseServer::SessionLoop() {
  auto fuse_state = FuseUpgradeManager::GetInstance().GetFuseState();
  if (fuse_state == FuseUpgradeState::kFuseUpgradeNew) {
    ProcessInitMsg();
  }

  // process user request
  if (FLAGS_fuse_use_single_thread) {
    return fuse_session_loop(session_);
  }

  fuse_loop_cfg_set_clone_fd(config_, FLAGS_fuse_use_clone_fd);
  fuse_loop_cfg_set_max_threads(config_, FLAGS_fuse_max_threads);
  return fuse_session_loop_mt(session_, config_);
}

void FuseServer::ExportMetrics(const std::string& key,
                               const std::string& value) {
  fd_comm_metrics_.set_value(value);
  fd_comm_metrics_.expose(butil::StringPiece(key));
}

int FuseServer::Serve() {
  // export fd_comm_path value for new dingo-client use
  ExportMetrics(kFdCommPathKey, fd_comm_file_);

  UdsServerStart();

  LOG(INFO) << fmt::format(
      "dingo-client start loop, singlethread={} max_threads={}.",
      FLAGS_fuse_use_single_thread, FLAGS_fuse_max_threads);

  if (SaveOpInitMsg() == 1) {
    LOG(ERROR) << "save fuse init message failed";
    return 1;
  }
  /* Block until ctrl+c or fusermount -u */
  int ret = SessionLoop();
  LOG(INFO) << "dingo-client is shutdown, ret=" << ret;

  return ret;
}

void FuseServer::SessionUnmount() {
  auto fuse_state = FuseUpgradeManager::GetInstance().GetFuseState();

  if (fuse_state == FuseUpgradeState::kFuseUpgradeOld) {
    LOG(INFO)
        << "during the smooth upgrade process, the filesystem not unmounted";
    return;
  }

  if (fuse_state == FuseUpgradeState::kFuseUpgradeNew) {
    // After smooth upgrade, fuse session will be umount by
    // DingoSessionUnmount instead of fuse_session_unmount because
    // session_->mountpoint is nullptr
    LOG(INFO) << "use DingoSessionUnmount";
    DingoSessionUnmount(mount_option_->mount_point, GetDevFd());
  } else {
    LOG(INFO) << "use fuse_session_unmount";
    fuse_session_unmount(session_);
  }
}

// TODO: check the fstype to determain the dingo-client
bool FuseServer::ShutdownGracefully(const std::string& mountpoint) {
  // get old dingo-client pid
  std::string file_name =
      absl::StrFormat("%s/%s", mountpoint, dingofs::kStatsName);

  int pid = 0;
  uint32_t retry = 0;
  do {
    pid = GetDingoFusePid(file_name);
    if (pid > 0) break;

    std::this_thread::sleep_for(
        std::chrono::milliseconds(FLAGS_fuse_fd_get_retry_interval_ms));
  } while (++retry <= FLAGS_fuse_fd_get_max_retries);

  if (pid <= 0) {
    LOG(ERROR) << "get pid fail, filepath=" << file_name;
    return false;
  }
  LOG(INFO) << "get pid success, pid=" << pid;

  FuseUpgradeManager::GetInstance().SetOldFusePid(pid);

  // recv mount fd、fuse_init data from old dingo-client
  std::string comm_path = GetFdCommFileName(file_name);
  CHECK(!comm_path.empty());
  LOG(INFO) << "get socket success, comm_path=" << comm_path;

  fuse_fd_ = GetFuseFd(comm_path.c_str(), init_fbuf_.mem, init_fbuf_.mem_size,
                       &init_fbuf_.size);
  for (int i = 0; i < FLAGS_fuse_fd_get_max_retries && fuse_fd_ <= 2; i++) {
    std::this_thread::sleep_for(
        std::chrono::milliseconds(FLAGS_fuse_fd_get_retry_interval_ms));
    fuse_fd_ = GetFuseFd(comm_path.c_str(), init_fbuf_.mem, init_fbuf_.mem_size,
                         &init_fbuf_.size);
  }
  if (fuse_fd_ <= 2) {
    LOG(ERROR) << "recv mount fd fail, comm_path=" << comm_path;
    return false;
  }
  LOG(INFO) << "recv data from " << comm_path << ", mount fd = " << fuse_fd_
            << ",data size = " << init_fbuf_.size;

  // send kill signal to old dingo-client
  kill(pid, SIGHUP);

  // check old dingo-client is alive
  for (int i = 0; i < FLAGS_fuse_check_alive_max_retries; i++) {
    std::this_thread::sleep_for(
        std::chrono::milliseconds(FLAGS_fuse_check_alive_retry_interval_ms));
    if (!CheckProcessAlive(pid)) {
      return true;
    }

    LOG(INFO) << "check old dingo-client is alive: YES";
  }

  return false;
}

}  // namespace fuse
}  // namespace client
}  // namespace dingofs
