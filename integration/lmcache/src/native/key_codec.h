/*
 * Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

#ifndef DINGOFS_LMCACHE_NATIVE_KEY_CODEC_H_
#define DINGOFS_LMCACHE_NATIVE_KEY_CODEC_H_

#include <string>

#include "common/block/tensor_key.h"

namespace dingofs {
namespace lmcache {

// Parse "model@world_size@worker_id@chunk_hash@dtype" into a TensorKey.
// Matches LMCache's CacheEngineKey.to_string() format and TensorKey::Filename().
// Throws std::invalid_argument if the input is malformed.
TensorKey ParseTensorKey(const std::string& s);

inline std::string EncodeTensorKey(const TensorKey& key) {
  return key.Filename();
}

}  // namespace lmcache
}  // namespace dingofs

#endif  // DINGOFS_LMCACHE_NATIVE_KEY_CODEC_H_
