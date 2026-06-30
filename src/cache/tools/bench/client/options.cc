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

#include "cache/tools/bench/client/options.h"

namespace dingofs {
namespace cache {
namespace bench {
namespace client {

void RegisterFlags(FlagSet* fs, Options* o) {
  fs->Str("op", &o->op_str, "operation: put | range");
  fs->Str("mds_addrs", &o->mds_addrs, "MDS address(es), e.g. 127.0.0.1:7400");
  fs->U32("fsid", &o->fs_id, "filesystem id");
  fs->U32("threads", &o->threads, "number of load-generator bthreads");
  fs->Size("block_size", &o->block_size, "whole block size");
  fs->Size("range_offset", &o->range_offset, "range-read offset in each block");
  fs->Size("range_length", &o->range_length, "range-read length; 0 = whole block");
  fs->Switch("writeback", &o->writeback, "put: stage to cache then write back");
  fs->Switch("retrieve_storage", &o->retrieve_storage,
             "range: fall back to storage on cache miss");
  fs->Real("qps", &o->qps, "target offered ops/sec; 0 = unlimited");
  fs->U32("max_inflight", &o->max_inflight, "max outstanding async ops");
  fs->U32("duration", &o->duration_s, "measurement seconds; 0 = use --ops");
  fs->U64("ops", &o->ops, "total ops to issue; 0 = unbounded");
  fs->U32("warmup", &o->warmup_s, "warmup seconds excluded from summary");
  fs->U64("keyspace", &o->keyspace, "number of distinct blocks");
  fs->Str("key_dist", &o->key_dist_str, "key distribution: seq | uniform | zipf");
  fs->Real("zipf_theta", &o->zipf_theta, "zipfian skew in (0, 1)");
  fs->U32("report_interval", &o->report_interval_s, "progress interval (s)");
  RegisterProfileFlags(fs, &o->profile);
}

std::string OperationName(OperationType op) {
  return op == OperationType::kPut ? "put" : "range";
}

std::string KeyDistName(KeyDist dist) {
  switch (dist) {
    case KeyDist::kSeq: return "seq";
    case KeyDist::kUniform: return "uniform";
    case KeyDist::kZipf: return "zipf";
  }
  return "unknown";
}

std::string Validate(Options* o) {
  if (o->op_str == "put") {
    o->operation = OperationType::kPut;
  } else if (o->op_str == "range") {
    o->operation = OperationType::kRange;
  } else {
    return "op must be put|range: " + o->op_str;
  }

  if (o->key_dist_str == "seq") {
    o->key_dist = KeyDist::kSeq;
  } else if (o->key_dist_str == "uniform") {
    o->key_dist = KeyDist::kUniform;
  } else if (o->key_dist_str == "zipf") {
    o->key_dist = KeyDist::kZipf;
  } else {
    return "key_dist must be seq|uniform|zipf: " + o->key_dist_str;
  }

  if (o->mds_addrs.empty()) return "--mds_addrs is required";
  if (o->threads == 0) return "threads must be > 0";
  if (o->block_size == 0) return "block_size must be > 0";
  if (o->range_offset >= o->block_size) {
    return "range_offset must be < block_size";
  }
  if (o->range_length == 0) {
    o->range_length = o->block_size - o->range_offset;
  }
  if (o->range_length > o->block_size - o->range_offset) {
    return "range_offset + range_length must fit in block_size";
  }
  if (o->keyspace == 0) return "keyspace must be > 0";
  if (o->qps < 0) return "qps must not be negative";
  if (o->key_dist == KeyDist::kZipf &&
      (o->zipf_theta <= 0.0 || o->zipf_theta >= 1.0)) {
    return "zipf_theta must be in the open range (0, 1)";
  }
  if (o->key_dist != KeyDist::kSeq && o->duration_s == 0 && o->ops == 0) {
    return "uniform/zipf need a stop condition: set --duration or --ops";
  }
  if (o->report_interval_s == 0) return "report_interval must be > 0";
  if (auto e = ValidateProfile(o->profile); !e.empty()) return e;
  return "";
}

}  // namespace client
}  // namespace bench
}  // namespace cache
}  // namespace dingofs
