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

#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <utility>

#include "absl/strings/str_format.h"
#include "client/fuse/fuse_common.h"
#include "client/fuse/fuse_lowlevel_ops_func.h"
#include "client/fuse/upgrade/handover_client.h"
#include "client/fuse/upgrade/handover_controller.h"
#include "client/fuse/upgrade/handover_server.h"
#include "client/fuse/upgrade/handover_session_impl.h"
#include "client/fuse/upgrade/state_store.h"
#include "common/directory.h"
#include "common/helper.h"
#include "common/status.h"
#include "fmt/format.h"
#include "fuse_opt.h"
#include "glog/logging.h"

using ::dingofs::utils::BufToHexString;

namespace dingofs {
namespace client {
namespace fuse {

// fuse mount options
DEFINE_string(fuse_mount_options, "default_permissions",
              "mount options for libfuse");

DEFINE_bool(fuse_use_single_thread, false, "use single thread for libfuse");
DEFINE_validator(fuse_use_single_thread, brpc::PassValidate);

DEFINE_bool(fuse_use_clone_fd, true, "use clone fd for libfuse");
DEFINE_validator(fuse_use_clone_fd, brpc::PassValidate);

DEFINE_uint32(fuse_max_threads, 64, "max threads for libfuse");
DEFINE_validator(fuse_max_threads, brpc::PassValidate);

// hot-upgrade controlled drain (#1): the old process drains in-flight FUSE
// requests and runs the flush+dump checkpoint before exiting on SIGHUP.
// Declared in common/options/client.h.
DEFINE_uint32(fuse_hotupgrade_drain_timeout_ms, 30000,
              "max time to wait for the session to drain before aborting the "
              "handover and resuming service on the old process");
DEFINE_uint32(fuse_hotupgrade_statfs_interval_ms, 50,
              "interval between statfs(2) round-trips used to wake workers "
              "blocked in read() while draining");

// ===========================================================================
// Construction / teardown
// ===========================================================================

FuseServer::FuseServer() = default;

int FuseServer::Init(const std::string& program_name,
                     struct MountOption* mount_option) {
  mount_option_ = mount_option;
  program_name_ = program_name;

  AllocateFuseInitBuf();

  if (AddMountOptions() == 1) return 1;

  config_ = fuse_loop_cfg_create();
  CHECK(config_ != nullptr) << "fuse_loop_cfg_create fail.";

  auto socket_path = GetDefaultDir(kSocketDir);
  CHECK(dingofs::Helper::CreateDirectory(socket_path))
      << "create directory" << socket_path << " fail";
  fd_comm_file_ =
      absl::StrFormat("%s/fd_comm_socket.%d", socket_path, getpid());

  // Old-side handover endpoint. Constructed here but only Start()ed in Serve()
  // after SaveOpInitMsg() fills init_fbuf_, so a connecting new process never
  // gets an empty INIT. get_dev_fd reads the /dev/fuse fd lazily (the session
  // is created later, in CreateSession).
  handover_server_ = std::make_unique<HandoverServer>(
      fd_comm_file_, [this]() { return GetDevFd(); }, &init_fbuf_);

  return 0;
}

FuseServer::~FuseServer() {
  unlink(fd_comm_file_.c_str());

  // Stop both handover threads BEFORE freeing init_fbuf_: the handover server's
  // accept loop reads &init_fbuf_ (init_msg_) when handing off, and the
  // controller drives the server. Stop controller first, then the server.
  if (handover_controller_) handover_controller_->Stop();
  if (handover_server_) handover_server_->Stop();

  FreeFuseInitBuf();
  fuse_opt_free_args(&args_);

  if (config_ != nullptr) {
    fuse_loop_cfg_destroy(config_);
  }

  auto fuse_state = UpgradeStateStore::GetInstance().GetFuseState();
  if (fuse_state == FuseUpgradeState::kFuseUpgradeOld) {
    LOG(INFO) << "transfer dingo-client session to others.";
  }
}

// ===========================================================================
// Mount-args / INIT-buffer helpers
// ===========================================================================

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

// ===========================================================================
// FUSE session lifecycle: CreateSession -> SessionMount -> Serve ->
// SessionUnmount -> DestroySession
// ===========================================================================

int FuseServer::CreateSession(void* usedata) {
  // create fuse new session
  session_ = fuse_session_new(&args_, &kFuseOp, sizeof(kFuseOp), usedata);
  if (session_ == nullptr) return 1;

  // install fuse signal
  if (fuse_set_signal_handlers(session_) != 0) return 1;

  return 0;
}

int FuseServer::SessionMount() {
  std::string mountpoint = mount_option_->mount_point;

  LOG(INFO) << fmt::format("Begin to mount fs {} to {}.",
                           mount_option_->fs_name, mountpoint);

  if (CanShutdownGracefully(mountpoint)) {
    // Take over the mount from the old dingo-client: receive its /dev/fuse fd
    // (and INIT message into init_fbuf_) and run the handover handshake.
    HandoverClient client;
    int fuse_fd = client.TakeOver(mountpoint, &init_fbuf_);
    if (fuse_fd <= 2) {
      LOG(ERROR) << "smooth upgrade failed, can't mount on: " << mountpoint;
      return 1;
    }
    LOG(INFO) << "old dingo-client is already shutdown";
    // new fuse processes
    UpgradeStateStore::GetInstance().UpdateFuseState(
        FuseUpgradeState::kFuseUpgradeNew);

    // Reuse the received /dev/fuse fd: mounting on /dev/fd/N attaches to the
    // existing kernel mount instead of creating a new one. OWNERSHIP: on a
    // successful fuse_session_mount below, libfuse adopts this fd as se->fd
    // (fuse_lowlevel.c: `se->fd = fd`, no dup) and closes it in
    // fuse_session_destroy. So FuseServer must NOT close it (would
    // double-close) nor hold it past mount. (On mount failure the process
    // aborts, so the fd leaks only in a terminal path.)
    mountpoint = absl::StrFormat("/dev/fd/%d", fuse_fd);
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
  auto fuse_state = UpgradeStateStore::GetInstance().GetFuseState();
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

    // fbuf.mem is allocated by the public fuse_session_receive_buf via a plain
    // malloc (internal=false); process_buf only reads it and never takes
    // ownership, so it must be freed here. Use free(), NOT fuse_buf_free():
    // the latter reverses an internal aligned-alloc offset and would free a
    // wrong pointer for an externally allocated buffer. INIT (~56B) is never
    // spliced, so the buffer is always a memory buffer.
    CHECK(!(fbuf.flags & FUSE_BUF_IS_FD)) << "INIT buf unexpectedly IS_FD";
    free(fbuf.mem);

    return 0;
  }

  // receive_buf may have allocated fbuf.mem before the read failed; free it on
  // the error path too (this branch previously leaked).
  if (fbuf.mem != nullptr) {
    free(fbuf.mem);
  }
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
  auto fuse_state = UpgradeStateStore::GetInstance().GetFuseState();
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

int FuseServer::Serve() {
  // export fd_comm_path value for new dingo-client use
  ExportMetrics(kFdCommPathKey, fd_comm_file_);

  LOG(INFO) << fmt::format(
      "dingo-client start loop, singlethread={} max_threads={}.",
      FLAGS_fuse_use_single_thread, FLAGS_fuse_max_threads);

  if (SaveOpInitMsg() == 1) {
    LOG(ERROR) << "save fuse init message failed";
    return 1;
  }

  // Start the handover endpoint only AFTER the INIT message is saved, so a new
  // process can never connect and receive an empty INIT. (init_fbuf_ is already
  // filled by the upgrade-takeover path for the new process.)
  handover_server_->Start();

  /* Block until ctrl+c or fusermount -u */
  int ret = SessionLoop();
  LOG(INFO) << "dingo-client is shutdown, ret=" << ret;

  return ret;
}

void FuseServer::SessionUnmount() {
  auto fuse_state = UpgradeStateStore::GetInstance().GetFuseState();

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

void FuseServer::DestroySession() {
  LOG(INFO) << "destroy dingo-client session.";

  // Stop the handover threads BEFORE destroying the session. The controller
  // drives the session (pause/resume/wait_drained/exit) and may still be mid
  // handover -- e.g. an external SIGTERM/SIGINT exits the loop while a drain is
  // in flight. Joining it here guarantees it never touches a freed session.
  // Stop is idempotent, so the dtor's later Stop is a no-op.
  if (handover_controller_) handover_controller_->Stop();
  if (handover_server_) handover_server_->Stop();

  if (session_ != nullptr) {
    fuse_remove_signal_handlers(session_);
    fuse_session_destroy(session_);
  }
}

// ===========================================================================
// Misc helpers
// ===========================================================================

int FuseServer::GetDevFd() const { return fuse_session_fd(session_); }

void FuseServer::Shutdown() {
  LOG(INFO) << "shutdown dingo-client";
  fuse_session_exit(session_);
}

void FuseServer::ExportMetrics(const std::string& key,
                               const std::string& value) {
  fd_comm_metrics_.set_value(value);
  fd_comm_metrics_.expose(butil::StringPiece(key));
}

// ===========================================================================
// Hot-upgrade coordination: arm the controller, register the flush checkpoint,
// and trigger a handover. The handover server (the controller's peer) and the
// handover client live in their own files.
// ===========================================================================

void FuseServer::ArmHandoverController() {
  CHECK(handover_controller_ == nullptr) << "handover controller already armed";

  HandoverOptions options;
  options.mountpoint = mount_option_->mount_point;
  options.drain_timeout_ms = FLAGS_fuse_hotupgrade_drain_timeout_ms;
  options.statfs_interval_ms = FLAGS_fuse_hotupgrade_statfs_interval_ms;
  session_handover_ = std::make_unique<HandoverSessionImpl>(session_);
  handover_controller_ = std::make_unique<HandoverController>(
      std::move(options), handover_server_.get(), session_handover_.get());
  handover_controller_->Start();
}

void FuseServer::SetHandoverCheckpoint(std::function<Status()> checkpoint) {
  // Called from FuseOpInit during Serve(), after ArmHandoverController().
  CHECK(handover_controller_ != nullptr) << "handover controller not armed";
  handover_controller_->SetCheckpoint(std::move(checkpoint));
}

void FuseServer::TriggerHandover() {
  // Called from the graceful signal thread on SIGHUP (normal context, not an
  // async signal handler). Just wakes the handover controller -- the controller
  // runs the actual drain -> checkpoint -> exit on its own thread. It is armed
  // in ArmHandoverController() before the signal thread starts, so it is always
  // present here.
  CHECK(handover_controller_ != nullptr) << "handover controller not armed";
  handover_controller_->RequestHandover();
}

}  // namespace fuse
}  // namespace client
}  // namespace dingofs
