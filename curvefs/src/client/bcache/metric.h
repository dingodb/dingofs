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

/*
 * Project: DingoFS
 * Created Date: 2024-08-19
 * Author: Jingli Chen (Wine93)
 */

#ifndef CURVEFS_SRC_CLIENT_BCACHE_METRIC_H_
#define CURVEFS_SRC_CLIENT_BCACHE_METRIC_H_

#include <bvar/bvar.h>

#include <string>

namespace curvefs {
namespace client {
namespace bcache {

class DiskCacheMetric {
 public:
    DiskCacheMetric(uint32_t index, const std::string& cacheDir) :
        metric_(StrFormat("block_cache.disk_caches[%d]", index)) {}

    void SetCacheStatus(const std::string& status) {
        metric_.status.set_value(status);
    }

    void AddStageBytes(uint64_t n) {
        metric_.stageBytes << n;
    }

    void AddCacheBytes(uint64_t n) {
        metric_.cacheBytes << n;
    }

 private:
    struct Metric {
        Metric(const std::string& prefix, const std::string& dir) :
            cacheDir(prefix, "cache_dir"),
            nstage(prefix, "nstage"),
            ncache(prefix, "ncache"),
            stageBytes(prefix, "stage_bytes"),
            cacheBytes(prefix, "cache_bytes"),
            cacheHit(prefix, "cache_hit"),
            cacheMiss(prefix, "cache_miss") {
            cacheDir.set_value(dir);
            status.set_value("unknown");
        }

        bvar::Status<std::string> cacheDir;
        bvar::Status<std::string> status;
        bvar::Adder<uint64_t> nstage;
        bvar::Adder<uint64_t> ncache;
        bvar::Adder<uint64_t> stageBytes;
        bvar::Adder<uint64_t> cacheBytes;
        bvar::Adder<uint64_t> cacheMiss;
        bvar::Adder<uint64_t> cacheHit;
    };

    Metric metric_;
};

str

struct MetricGuard {
    explicit CodeGuard(CURVEFS_ERROR* rc, bvar::Adder<uint64_t>* ecount)
    : rc_(rc), ecount_(ecount) {}

    ~CodeGuard() {
        if (*rc_ != CURVEFS_ERROR::OK) {
            (*ecount_) << 1;
        }
    }

    CURVEFS_ERROR* rc_;
    bvar::Adder<uint64_t>* ecount_;
};

}  // namespace bcache
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_BCACHE_METRIC_H_
