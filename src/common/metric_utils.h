/*
 *  Copyright (c) 2022 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
 * Project: dingo
 * Date: Tuesday Apr 05 15:42:46 CST 2022
 * Author: wuhanqing
 */

#ifndef DINGOFS_SRC_COMMON_METRIC_UTILS_H_
#define DINGOFS_SRC_COMMON_METRIC_UTILS_H_

#include <butil/time.h>
#include <bvar/bvar.h>

#include <vector>
namespace dingofs {
namespace common {

struct LatencyUpdater {
  explicit LatencyUpdater(bvar::LatencyRecorder* recorder)
      : recorder(recorder) {
    timer.start();
  }

  ~LatencyUpdater() {
    timer.stop();
    (*recorder) << timer.u_elapsed();
  }

  bvar::LatencyRecorder* recorder;
  butil::Timer timer;
};

struct LatencyListUpdater {
  explicit LatencyListUpdater(std::vector<bvar::LatencyRecorder*> recorderList)
      : recorderList(recorderList) {
    timer.start();
  }

  ~LatencyListUpdater() {
    timer.stop();
    int64_t u_elapsed = timer.u_elapsed();
    for (auto& recorder : recorderList) {
      (*recorder) << u_elapsed;
    }
  }
  std::vector<bvar::LatencyRecorder*> recorderList;
  butil::Timer timer;
};

struct InflightGuard {
  explicit InflightGuard(bvar::Adder<int64_t>* inflight) : inflight_(inflight) {
    (*inflight_) << 1;
  }

  ~InflightGuard() { (*inflight_) << -1; }

  bvar::Adder<int64_t>* inflight_;
};

}  // namespace common
}  // namespace dingofs

#endif  // DINGOFS_SRC_COMMON_METRIC_UTILS_H_
