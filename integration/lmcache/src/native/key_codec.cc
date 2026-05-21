/*
 * Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

#include "key_codec.h"

#include <cstring>
#include <stdexcept>
#include <string>
#include <vector>

namespace dingofs {
namespace lmcache {

TensorKey ParseTensorKey(const std::string& s) {
  // Five segments separated by '@': model@ws@wid@hash@dtype.
  // model_name may itself contain '@' would break parsing — we forbid it
  // implicitly by splitting from the right for the trailing four fixed parts.
  std::vector<std::string> rev_segments;
  rev_segments.reserve(4);
  size_t end = s.size();
  for (int i = 0; i < 4; ++i) {
    size_t at = s.rfind('@', end == 0 ? 0 : end - 1);
    if (at == std::string::npos) {
      throw std::invalid_argument("TensorKey missing '@' separators: " + s);
    }
    rev_segments.push_back(s.substr(at + 1, end - at - 1));
    end = at;
  }
  if (end == 0) {
    throw std::invalid_argument("TensorKey missing model_name: " + s);
  }
  TensorKey key;
  key.model_name = s.substr(0, end);
  // rev_segments order: dtype, hash, wid, ws
  key.dtype = std::move(rev_segments[0]);
  key.chunk_hash = std::move(rev_segments[1]);
  key.worker_id = static_cast<uint32_t>(std::stoul(rev_segments[2]));
  key.world_size = static_cast<uint32_t>(std::stoul(rev_segments[3]));
  return key;
}

}  // namespace lmcache
}  // namespace dingofs
