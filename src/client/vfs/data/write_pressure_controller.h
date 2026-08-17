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

#ifndef DINGOFS_CLIENT_VFS_DATA_WRITE_PRESSURE_CONTROLLER_H_
#define DINGOFS_CLIENT_VFS_DATA_WRITE_PRESSURE_CONTROLLER_H_

#include <condition_variable>
#include <cstdint>
#include <mutex>

#include "bvar/reducer.h"
#include "common/status.h"
#include "common/writemempool/write_pressure_observer.h"

namespace dingofs {

class Executor;

namespace client {
namespace vfs {

class WriterTable;

// Event-driven control plane for write-page pressure. It coalesces pressure
// events into at most one running and one pending round, then asks WriterTable
// to fan out dirty writers. It does not rate-limit backend PUT traffic.
class WritePressureController final : public WritePressureObserver {
 public:
  WritePressureController(WriterTable* writer_table, Executor* executor);
  ~WritePressureController() override;

  WritePressureController(const WritePressureController&) = delete;
  WritePressureController& operator=(const WritePressureController&) = delete;

  // May be called from foreground writers and upload completion threads. It
  // only mutates the small controller state and submits work to executor.
  void OnWritePressure() override;

  // Rejects new events and waits for the active round, its writer callbacks,
  // and any bounded submit retry to finish. The executor and WriterTable must
  // remain alive until this returns.
  void StopAndDrain();

 private:
  static constexpr uint32_t kMaxSubmitRetries = 3;
  static constexpr int kInitialRetryDelayMs = 10;

  void SubmitRound(uint32_t attempt);
  void RunRound();
  void OnRoundDone(uint64_t event_snapshot, Status status);
  void FinishSubmitFailure();

  WriterTable* writer_table_{nullptr};
  Executor* executor_{nullptr};

  std::mutex mutex_;
  std::condition_variable cv_;
  bool running_{false};
  bool pending_{false};
  bool stopped_{false};
  uint64_t event_epoch_{0};

  bvar::Adder<int64_t> event_num_;
  bvar::Adder<int64_t> coalesced_event_num_;
  bvar::Adder<int64_t> round_num_;
  bvar::Adder<int64_t> round_failure_num_;
  bvar::Adder<int64_t> submit_failure_num_;
};

}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_CLIENT_VFS_DATA_WRITE_PRESSURE_CONTROLLER_H_
