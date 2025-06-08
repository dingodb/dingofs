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
 * Created Date: 2024-09-23
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_METRICS_BLOCK_CACHE_METRIC_H_
#define DINGOFS_SRC_CACHE_METRICS_BLOCK_CACHE_METRIC_H_

namespace dingofs {
namespace cache {

/*
class BlockCacheMetricHelper {
public:
 static void PrintOnPending(std::ostream& os, void* arg) {
   auto pending_queue =
       reinterpret_cast<BlockCacheUploader*>(arg)->pending_queue_;

   struct BlocksStat stat;
   pending_queue->Stat(&stat);
   os << stat.num_total << "," << stat.num_from_cto << ","
      << stat.num_from_nocto << "," << stat.num_from_reload;
 }

 static void PrintOnUploading(std::ostream& os, void* arg) {
   auto uploading_queue =
       reinterpret_cast<BlockCacheUploader*>(arg)->uploading_queue_;

   struct BlocksStat stat;
   uploading_queue->Stat(&stat);
   os << stat.num_total << "," << stat.num_from_cto << ","
      << stat.num_from_nocto << "," << stat.num_from_reload;
 }

 static bool IsThrottleEnable(void*) {
   return FLAGS_block_cache_stage_bandwidth_throttle_enable;
 }

  static uint64_t GetThrottleLimit(void* arg) {
    auto* throttle = reinterpret_cast<BlockCacheThrottle*>(arg);
    return throttle->current_bandwidth_throttle_mb_;
  }

  static bool IsThrottleOverflow(void* arg) {
    auto* throttle = reinterpret_cast<BlockCacheThrottle*>(arg);
    return throttle->waiting_;
};

class BlockCacheMetric {
 public:
  struct AuxMember {
    AuxMember(std::shared_ptr<BlockCacheUploader> uploader)
        : uploader(uploader) {}

    std::shared_ptr<BlockCacheUploader> uploader;
  };

  BlockCacheMetric(BlockCacheOption option, AuxMember aux_members)
      : metric_("dingofs_block_cache", aux_members) {
  metric_.upload_stage_workers.set_value(option.upload_stage_workers());
  metric_.upload_stage_queue_capacity.set_value(
      option.upload_stage_queue_size());
}

virtual ~BlockCacheMetric() = default;

private:
struct Metric {
  Metric(const std::string& prefix, AuxMember aux_members)
      :  // upload stage
        upload_stage_workers(prefix, "upload_stage_workers", 0),
        upload_stage_queue_capacity(prefix, "upload_stage_queue_capacity", 0),
        stage_blocks_on_pending(prefix, "stage_blocks_on_pending",
                                &BlockCacheMetricHelper::PrintOnPending,
                                aux_members.uploader.get()),
        stage_blocks_on_uploading(prefix, "stage_blocks_on_uploading",
                                  &BlockCacheMetricHelper::PrintOnUploading,
                                  aux_members.uploader.get()) {}
  stage bandwidth throttle stage_bandwidth_throttle_enable(
      prefix, "stage_bandwidth_throttle_enable",
      &BlockCacheMetricHelper::IsThrottleEnable, aux_members.throttle.get()),
      stage_bandwidth_throttle_mb(prefix, "stage_bandwidth_throttle_mb",
                                  &BlockCacheMetricHelper::GetThrottleLimit,
                                  aux_members.throttle.get()),
      stage_bandwidth_throttle_overflow(
          prefix, "stage_bandwidth_throttle_overflow",
          &BlockCacheMetricHelper::IsThrottleOverflow,
aux_members.throttle.get()) {}

  bvar::Status<uint32_t> upload_stage_workers;
  bvar::Status<uint32_t> upload_stage_queue_capacity;
  bvar::PassiveStatus<std::string> stage_blocks_on_pending;
  bvar::PassiveStatus<std::string> stage_blocks_on_uploading;
  bvar::PassiveStatus<bool> stage_bandwidth_throttle_enable;
  bvar::PassiveStatus<uint64_t> stage_bandwidth_throttle_mb;
  bvar::PassiveStatus<bool> stage_bandwidth_throttle_overflow;
};

bvar::Status<uint32_t> upload_stage_workers_;
bvar::Status<uint32_t> upload_stage_queue_capacity_;
bvar::PassiveStatus<std::string> stage_blocks_on_pending_;
bvar::PassiveStatus<std::string> stage_blocks_on_uploading_;
bvar::PassiveStatus<bool> stage_bandwidth_throttle_enable;
bvar::PassiveStatus<uint64_t> stage_bandwidth_throttle_mb;
bvar::PassiveStatus<bool> stage_bandwidth_throttle_overflow;
};
*/

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_METRICS_BLOCK_CACHE_METRIC_H_
