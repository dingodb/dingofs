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

#include "client/fuse/upgrade/handover_controller.h"

#include <sys/vfs.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <thread>
#include <utility>

#include "client/fuse/upgrade/state_store.h"
#include "fmt/format.h"
#include "glog/logging.h"

namespace dingofs {
namespace client {
namespace fuse {

class HandoverController::HandoverRequestEvent {
 public:
  void Notify() {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      notified_ = true;
    }
    cond_.notify_one();
  }

  void Wait() {
    std::unique_lock<std::mutex> lock(mutex_);
    cond_.wait(lock, [this]() { return notified_; });
    notified_ = false;
  }

 private:
  std::mutex mutex_;
  std::condition_variable cond_;
  bool notified_{false};
};

namespace {

class StatfsWakeupLoop {
 public:
  void Start(const std::string& mountpoint, uint32_t interval_ms,
             HandoverOptions::StatfsWakeupFn wakeup_fn) {
    if (!wakeup_fn) {
      wakeup_fn = [](const std::string& mountpoint,
                     const std::atomic<bool>&) {
        struct statfs buf;
        (void)statfs(mountpoint.c_str(), &buf);
      };
    }

    stop_ = std::make_shared<std::atomic<bool>>(false);
    auto stop = stop_;
    thread_ = std::thread([stop, mountpoint, interval_ms,
                           wakeup_fn = std::move(wakeup_fn)]() {
      while (!stop->load()) {
        // Drive a FUSE round-trip to pop a worker blocked in read() back to the
        // recv_paused check. pause_receive() does not wake a blocked reader.
        wakeup_fn(mountpoint, *stop);
        std::this_thread::sleep_for(std::chrono::milliseconds(interval_ms));
      }
    });
  }

  void StopAndDetach() {
    Stop();
    if (thread_.joinable()) thread_.detach();
  }

  ~StatfsWakeupLoop() {
    Stop();
    if (thread_.joinable()) thread_.detach();
  }

 private:
  void Stop() {
    if (stop_) stop_->store(true);
  }

  std::shared_ptr<std::atomic<bool>> stop_;
  std::thread thread_;
};

}  // namespace

HandoverController::HandoverController(HandoverOptions options,
                                       HandoverPeer* peer,
                                       HandoverSession* session)
    : options_(std::move(options)),
      peer_(peer),
      session_(session),
      request_event_(std::make_unique<HandoverRequestEvent>()) {
  CHECK(peer_ != nullptr) << "handover peer must not be null";
  CHECK(session_ != nullptr) << "handover session must not be null";
}

HandoverController::~HandoverController() { Stop(); }

void HandoverController::SetCheckpoint(HandoverCheckpoint checkpoint) {
  std::lock_guard<std::mutex> lock(checkpoint_mutex_);
  checkpoint_ = std::move(checkpoint);
}

void HandoverController::Start() {
  if (started_) return;
  started_ = true;
  thread_ = std::thread(&HandoverController::Run, this);
}

void HandoverController::Stop() {
  if (!started_) return;
  shutdown_.store(true);
  request_event_->Notify();
  if (thread_.joinable()) thread_.join();
  started_ = false;
}

bool HandoverController::RequestHandover() {
  if (!started_) return false;
  request_event_->Notify();
  return true;
}

bool HandoverController::WaitForRequest() {
  request_event_->Wait();
  return !shutdown_.load();
}

void HandoverController::Run() {
  while (WaitForRequest()) {
    // SIGHUP only wakes us; the real trigger is a kPrepare message from the new
    // process on a live UDS connection. peer_->WaitHandoverPrepare() returns
    // false for a stray SIGHUP (no connection, or a dead/silent peer), in which
    // case we
    // must NOT drain and tear down the VFS -- there would be no peer to hand
    // off to and no way to resume past the checkpoint. Stay serving instead.
    if (!peer_->WaitHandoverPrepare()) {
      LOG(ERROR) << "hot-upgrade: SIGHUP without a valid handover request; "
                    "ignoring and keep serving";
      continue;
    }

    // Do NOT pause IO (drain) if there is nothing to commit yet. The checkpoint
    // is registered by FuseOpInit during Serve(); a SIGHUP between arming this
    // controller and that registration -- or after g_vfs->Start() failed and it
    // is never registered -- would otherwise drain a full round only to abort.
    // Tell the new to back off and keep serving without ever pausing.
    bool has_checkpoint;
    {
      std::lock_guard<std::mutex> lock(checkpoint_mutex_);
      has_checkpoint = static_cast<bool>(checkpoint_);
    }

    if (!has_checkpoint) {
      LOG(ERROR) << "hot-upgrade: handover checkpoint not registered yet; "
                    "ignoring SIGHUP, keep serving";
      peer_->NotifyHandoverAbort();
      continue;
    }

    // Mark kFuseUpgradeOld here, in this normal worker thread -- NOT in the
    // SIGHUP handler. UpdateFuseState takes a mutex and is not
    // async-signal-safe; running it from the handler risks a self-deadlock if
    // the interrupted thread already holds that mutex. AbortHandover() reverts
    // it on failure.
    UpgradeStateStore::GetInstance().UpdateFuseState(
        FuseUpgradeState::kFuseUpgradeOld);
    LOG(INFO) << "hot-upgrade: handover requested, begin controlled drain";

    Status s = DrainSessionForHandover();
    if (!s.ok()) {
      AbortHandover(s);
      continue;
    }

    // Today the registered checkpoint tears down VFS and dumps state; if that
    // stop/dump step fails it LOG(FATAL)s because we are already past the clean
    // rollback point. This non-OK path is still intentional for two cases:
    // missing/unregistered checkpoint, and the future resumable checkpoint
    // design where pre-teardown flush/dump can fail and old should resume
    // serving.
    s = RunCheckpoint();
    if (!s.ok()) {
      AbortHandover(s);
      continue;
    }

    CommitHandover();
    return;
  }
}

Status HandoverController::DrainSessionForHandover() {
  session_->PauseReceive();

  StatfsWakeupLoop wakeup;
  wakeup.Start(options_.mountpoint, options_.statfs_interval_ms,
               options_.statfs_wakeup_fn);
  int rc = session_->WaitDrained(options_.drain_timeout_ms);

  if (rc != 0) {
    // Leave the session paused; the caller's AbortHandover() does the single
    // ResumeReceive() (and state rollback). The statfs wakeup thread may itself
    // be blocked in a FUSE statfs request queued behind the paused session. Never
    // join it here: on timeout that can deadlock the controller before it sends
    // kNack/resumes receive. Stop it and detach; after ResumeReceive() serves the
    // pending statfs, the thread observes stop_ and exits.
    wakeup.StopAndDetach();
    return Status::Internal(
        fmt::format("wait_drained failed rc={} (0=ok, -1=timeout)", rc));
  }

  // Drained: the wakeup thread may be blocked in a statfs the paused session
  // will no longer serve. Detach it; it holds no FuseServer references and exits
  // once the new process serves the pending statfs, or with this process on
  // commit.
  wakeup.StopAndDetach();
  return Status::OK();
}

Status HandoverController::RunCheckpoint() {
  HandoverCheckpoint checkpoint;
  {
    std::lock_guard<std::mutex> lock(checkpoint_mutex_);
    checkpoint = checkpoint_;
  }
  if (!checkpoint) {
    return Status::InvalidParam(
        "hot-upgrade handover checkpoint is not registered");
  }
  return checkpoint();
}

void HandoverController::AbortHandover(const Status& status) {
  peer_->NotifyHandoverAbort();
  session_->ResumeReceive();
  UpgradeStateStore::GetInstance().UpdateFuseState(
      FuseUpgradeState::kFuseNormal);
  LOG(ERROR) << "hot-upgrade: abort handover, old process keeps serving, "
             << "status: " << status.ToString();
}

void HandoverController::CommitHandover() {
  // NotifyReadyToExit() sends kReadyToExit ONCE. We are past the checkpoint
  // here: the VFS is already stopped + dumped and cannot be resumed, so there
  // is no rollback if the new is gone or cannot receive the notification.
  //
  // On notify failure (new died after kPrepare or closed the UDS), log and exit
  // anyway: hanging forever holding a drained mount is worse than exiting, and
  // exiting lets the kernel abort the orphaned mount so a remount/restart can
  // recover. Do NOT loop-retry NotifyReadyToExit(): it would re-send
  // kReadyToExit, which the new reads only once. A stronger handover needs the
  // checkpoint to dump-without-teardown and wait for a post-load ACK.
  if (!peer_->NotifyReadyToExit()) {
    LOG(ERROR) << "hot-upgrade: failed to notify new after teardown; exiting "
                  "anyway, mount may be orphaned until remount";
  } else {
    LOG(INFO) << "hot-upgrade: new notified ready, exit session for handover";
  }

  session_->Exit();
}

}  // namespace fuse
}  // namespace client
}  // namespace dingofs
