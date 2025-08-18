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

/*
 * Project: DingoFS
 * Created Date: 2025-06-04
 * Author: Jingli Chen (Wine93)
 */

#include "cache/benchmark/worker.h"

#include "cache/benchmark/option.h"

namespace dingofs {
namespace cache {

Worker::Worker(uint64_t idx, TaskFactorySPtr factory, CollectorSPtr collector)
    : idx_(idx),
      factory_(factory),
      collector_(collector),
      countdown_(FLAGS_blocks) {}

void Worker::Start() { ExecAllTasks(); }

void Worker::Shutdown() { countdown_.wait(); }

void Worker::ExecAllTasks() {
  BlockKeyIterator iter(idx_, FLAGS_fsid, FLAGS_ino, FLAGS_blksize,
                        FLAGS_blocks);

  for (iter.SeekToFirst(); iter.Valid(); iter.Next()) {
    auto task = factory_->GenTask(iter.Key());
    ExecTask(task);

    VLOG(9) << "Execute task (key=" << iter.Key().Filename() << ").";

    countdown_.signal(1);
  }
}

void Worker::ExecTask(std::function<void()> task) {
  butil::Timer timer;

  timer.start();
  task();
  timer.stop();

  collector_->Submit([this, timer](Stat* stat, Stat* total) {
    stat->Add(FLAGS_blksize, timer.u_elapsed());
    total->Add(FLAGS_blksize, timer.u_elapsed());
  });
}

}  // namespace cache
}  // namespace dingofs
