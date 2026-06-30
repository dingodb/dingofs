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

#ifndef DINGOFS_SRC_CACHE_TOOLS_FLAMEGRAPH_FLAMEGRAPH_H_
#define DINGOFS_SRC_CACHE_TOOLS_FLAMEGRAPH_FLAMEGRAPH_H_

#include <cstdint>
#include <istream>
#include <string>

namespace dingofs {
namespace cache {
namespace flamegraph {

enum class Palette : uint8_t { kHot, kIo };

struct FlameOptions {
  std::string title{"Flame Graph"};
  std::string countname{"samples"};  // unit shown in tooltips
  Palette palette{Palette::kHot};
  int width{1600};            // image width in pixels
  double minwidth_px{0.1};    // prune frames narrower than this
};

// Fold `perf script` text (read from `in`) into collapsed stacks and write
// "frame;frame;... count" lines to `folded_out`. A native, dependency-free
// reimplementation of stackcollapse-perf.pl's common path. Returns false on
// io error.
bool CollapsePerfScript(std::istream& in, const std::string& folded_out);

// Render collapsed stacks (`folded_in`) into a self-contained, interactive SVG
// flame graph at `svg_out`. A native reimplementation of flamegraph.pl: same
// layout and the same embedded browser-side script. Returns false on io error
// or when the input has no stacks.
bool RenderFlameSvg(const std::string& folded_in, const std::string& svg_out,
                    const FlameOptions& options);

}  // namespace flamegraph
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_TOOLS_FLAMEGRAPH_FLAMEGRAPH_H_
