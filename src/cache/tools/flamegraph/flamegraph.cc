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

// Native reimplementation of stackcollapse-perf.pl + flamegraph.pl (layout,
// colors, SVG structure), so cb renders flame graphs with no perl at runtime.
// The geometry and color math mirror flamegraph.pl
// (https://github.com/brendangregg/FlameGraph, CDDL-1.0); the interactive
// browser-side script it embeds lives verbatim in flamegraph_assets.cc.

#include "cache/tools/flamegraph/flamegraph.h"

#include <algorithm>
#include <cctype>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <map>
#include <sstream>
#include <string>
#include <vector>

namespace dingofs {
namespace cache {
namespace flamegraph {

// Defined in flamegraph_assets.cc: the interactive <script> body (CDDL-1.0,
// lifted from flamegraph.pl).
extern const char kFlameGraphJs[];

namespace {

// Layout constants, matching flamegraph.pl defaults.
constexpr int kXpad = 10;          // left/right padding
constexpr int kFrameHeight = 16;   // per-frame height
constexpr int kFramePad = 1;       // vertical gap between frames
constexpr int kFontSize = 12;
constexpr double kFontWidth = 0.59;  // avg glyph width / font size
constexpr int kYpad1 = kFontSize * 3;       // top pad (title)
constexpr int kYpad2 = kFontSize * 2 + 10;  // bottom pad (labels)
constexpr int kTitleSize = kFontSize + 5;

// ---- stackcollapse ---------------------------------------------------------

// Pull the function name out of one `perf script` stack line, e.g.
//   "    7fab12 std::sort<...>+0x1f (/lib/x.so)"  ->  "std::sort<...>"
std::string ParseFrame(const std::string& line) {
  size_t b = line.find_first_not_of(" \t");
  if (b == std::string::npos) return "";
  std::string s = line.substr(b);
  // drop the leading instruction address
  size_t sp = s.find(' ');
  if (sp == std::string::npos) return "[unknown]";
  std::string rest = s.substr(sp + 1);
  // drop the trailing " (module)"
  size_t paren = rest.rfind(" (");
  if (paren != std::string::npos) rest = rest.substr(0, paren);
  // drop a trailing "+0x..." offset
  size_t plus = rest.rfind('+');
  if (plus != std::string::npos && plus + 2 < rest.size() &&
      rest[plus + 1] == '0' && rest[plus + 2] == 'x') {
    rest = rest.substr(0, plus);
  }
  while (!rest.empty() && (rest.back() == ' ' || rest.back() == '\t')) {
    rest.pop_back();
  }
  if (rest.empty()) return "[unknown]";
  std::replace(rest.begin(), rest.end(), ';', ':');  // keep folded separator safe
  return rest;
}

// comm = the text before the first " <pid>" field in a sample header line.
std::string ParseComm(const std::string& line) {
  size_t cut = std::string::npos;
  for (size_t i = 0; i + 1 < line.size(); ++i) {
    if ((line[i] == ' ' || line[i] == '\t') &&
        (std::isdigit(static_cast<unsigned char>(line[i + 1])) != 0)) {
      cut = i;
      break;
    }
  }
  std::string comm = cut == std::string::npos ? line : line.substr(0, cut);
  size_t e = comm.find_last_not_of(" \t");
  comm = e == std::string::npos ? "" : comm.substr(0, e + 1);
  std::replace(comm.begin(), comm.end(), ' ', '_');
  std::replace(comm.begin(), comm.end(), ';', ':');
  return comm.empty() ? "perf" : comm;
}

bool IsBlank(const std::string& s) {
  return s.find_first_not_of(" \t\r\n") == std::string::npos;
}

}  // namespace

bool CollapsePerfScript(std::istream& in, const std::string& folded_out) {
  std::map<std::string, uint64_t> stacks;
  std::string comm;
  std::vector<std::string> frames;  // leaf-first, as perf prints them

  auto flush = [&]() {
    if (comm.empty() && frames.empty()) return;
    std::string folded = comm.empty() ? "perf" : comm;
    for (auto it = frames.rbegin(); it != frames.rend(); ++it) {
      folded += ';';
      folded += *it;
    }
    ++stacks[folded];
    comm.clear();
    frames.clear();
  };

  std::string line;
  while (std::getline(in, line)) {
    if (IsBlank(line)) {
      flush();
    } else if (line[0] == ' ' || line[0] == '\t') {
      frames.push_back(ParseFrame(line));
    } else {
      // header line of a new sample
      flush();
      comm = ParseComm(line);
    }
  }
  flush();

  std::ofstream out(folded_out);
  if (!out) return false;
  for (const auto& [stack, count] : stacks) {
    out << stack << ' ' << count << '\n';
  }
  return static_cast<bool>(out);
}

// ---- render ----------------------------------------------------------------

namespace {

struct Node {
  uint64_t total = 0;
  std::map<std::string, Node> children;  // sorted -> deterministic x layout
};

struct Frame {
  int depth;
  uint64_t stime;
  uint64_t etime;
  std::string func;
};

// flamegraph.pl namehash: stable per-name vector in [0,1], early chars weighted.
double NameHash(std::string name) {
  size_t bt = name.find('`');
  if (bt != std::string::npos) name = name.substr(bt + 1);
  double vec = 0, weight = 1, max = 1;
  int mod = 10;
  for (char c : name) {
    int i = static_cast<unsigned char>(c) % mod;
    vec += (static_cast<double>(i) / (mod - 1)) * weight;
    max += weight;
    weight *= 0.70;
    ++mod;
    if (mod > 12) break;
  }
  return 1.0 - vec / max;
}

std::string Color(Palette palette, const std::string& func) {
  if (func == "--") return "rgb(160,160,160)";
  if (func == "-") return "rgb(200,200,200)";
  double v1 = NameHash(func);
  std::string rev(func.rbegin(), func.rend());
  double vr = NameHash(rev);
  int r, g, b;
  if (palette == Palette::kIo) {
    r = 80 + static_cast<int>(60 * v1);
    g = r;
    b = 190 + static_cast<int>(55 * vr);
  } else {  // kHot
    r = 205 + static_cast<int>(50 * vr);
    g = static_cast<int>(230 * v1);
    b = static_cast<int>(55 * vr);
  }
  return "rgb(" + std::to_string(r) + ',' + std::to_string(g) + ',' +
         std::to_string(b) + ')';
}

std::string EscapeXml(const std::string& s, bool quotes) {
  std::string out;
  out.reserve(s.size());
  for (char c : s) {
    switch (c) {
      case '&': out += "&amp;"; break;
      case '<': out += "&lt;"; break;
      case '>': out += "&gt;"; break;
      case '"': out += quotes ? "&quot;" : "\""; break;
      default: out += c;
    }
  }
  return out;
}

std::string Commas(uint64_t n) {
  std::string s = std::to_string(n);
  for (int i = static_cast<int>(s.size()) - 3; i > 0; i -= 3) s.insert(i, ",");
  return s;
}

std::string F1(double v) {
  char buf[32];
  std::snprintf(buf, sizeof(buf), "%.1f", v);
  return buf;
}
std::string F2(double v) {
  char buf[32];
  std::snprintf(buf, sizeof(buf), "%.2f", v);
  return buf;
}

void Walk(const Node& node, int depth, uint64_t stime, const std::string& name,
          uint64_t min_samples, int& depthmax, std::vector<Frame>& out) {
  uint64_t etime = stime + node.total;
  if (depth > 0 && node.total < min_samples) return;  // prune narrow subtrees
  out.push_back({depth, stime, etime, name});
  depthmax = std::max(depthmax, depth);
  uint64_t offset = stime;
  for (const auto& [cname, child] : node.children) {
    Walk(child, depth + 1, offset, cname, min_samples, depthmax, out);
    offset += child.total;
  }
}

}  // namespace

bool RenderFlameSvg(const std::string& folded_in, const std::string& svg_out,
                    const FlameOptions& options) {
  // Build the prefix tree from folded stacks.
  std::ifstream in(folded_in);
  if (!in) return false;
  Node root;
  std::string line;
  while (std::getline(in, line)) {
    size_t sp = line.rfind(' ');
    if (sp == std::string::npos) continue;
    uint64_t count = std::strtoull(line.c_str() + sp + 1, nullptr, 10);
    if (count == 0) continue;
    std::string stack = line.substr(0, sp);
    root.total += count;
    Node* cur = &root;
    size_t start = 0;
    while (start <= stack.size()) {
      size_t semi = stack.find(';', start);
      std::string frame = stack.substr(
          start, semi == std::string::npos ? std::string::npos : semi - start);
      if (!frame.empty()) {
        cur = &cur->children[frame];
        cur->total += count;
      }
      if (semi == std::string::npos) break;
      start = semi + 1;
    }
  }

  const uint64_t total = root.total;
  if (total == 0) return false;

  const int width = options.width;
  const double width_per_time = (width - 2.0 * kXpad) / total;
  const uint64_t min_samples = width_per_time > 0
      ? static_cast<uint64_t>(options.minwidth_px / width_per_time)
      : 0;

  int depthmax = 0;
  std::vector<Frame> frames;
  Walk(root, 0, 0, "", min_samples, depthmax, frames);

  const int height = (depthmax + 1) * kFrameHeight + kYpad1 + kYpad2;
  const char* bg2 = options.palette == Palette::kIo ? "#e0e0ff" : "#eeeeb0";

  std::ofstream svg(svg_out);
  if (!svg) return false;

  svg << "<?xml version=\"1.0\" standalone=\"no\"?>\n"
      << "<!DOCTYPE svg PUBLIC \"-//W3C//DTD SVG 1.1//EN\" "
         "\"http://www.w3.org/Graphics/SVG/1.1/DTD/svg11.dtd\">\n"
      << "<svg version=\"1.1\" width=\"" << width << "\" height=\"" << height
      << "\" onload=\"init(evt)\" viewBox=\"0 0 " << width << ' ' << height
      << "\" xmlns=\"http://www.w3.org/2000/svg\" "
         "xmlns:xlink=\"http://www.w3.org/1999/xlink\">\n"
      << "<defs><linearGradient id=\"background\" y1=\"0\" y2=\"1\" x1=\"0\" "
         "x2=\"0\"><stop stop-color=\"#eeeeee\" offset=\"5%\"/><stop "
         "stop-color=\""
      << bg2 << "\" offset=\"95%\"/></linearGradient></defs>\n"
      << "<style type=\"text/css\">\n"
         "\ttext { font-family:Verdana; font-size:12px; fill:rgb(0,0,0); }\n"
         "\t#search, #ignorecase { opacity:0.1; cursor:pointer; }\n"
         "\t#search:hover, #search.show, #ignorecase:hover, #ignorecase.show { "
         "opacity:1; }\n"
         "\t#subtitle { text-anchor:middle; font-color:rgb(160,160,160); }\n"
         "\t#title { text-anchor:middle; font-size:"
      << kTitleSize << "px}\n"
      << "\t#unzoom { cursor:pointer; }\n"
         "\t#frames > *:hover { stroke:black; stroke-width:0.5; cursor:pointer; "
         "}\n"
         "\t.hide { display:none; }\n"
         "\t.parent { opacity:0.5; }\n"
         "</style>\n"
      << "<script type=\"text/ecmascript\"><![CDATA[" << kFlameGraphJs
      << "]]></script>\n";

  // canvas + UI chrome
  svg << "<rect x=\"0\" y=\"0\" width=\"" << F1(width) << "\" height=\""
      << F1(height) << "\" fill=\"url(#background)\"/>\n";
  svg << "<text id=\"title\" x=\"" << F2(width / 2.0) << "\" y=\""
      << kFontSize * 2 << "\">" << EscapeXml(options.title, false)
      << "</text>\n";
  svg << "<text id=\"details\" x=\"" << F2(kXpad) << "\" y=\""
      << height - kYpad2 / 2 << "\"> </text>\n";
  svg << "<text id=\"unzoom\" x=\"" << F2(kXpad) << "\" y=\"" << kFontSize * 2
      << "\" class=\"hide\">Reset Zoom</text>\n";
  svg << "<text id=\"search\" x=\"" << F2(width - kXpad - 100) << "\" y=\""
      << kFontSize * 2 << "\">Search</text>\n";
  svg << "<text id=\"ignorecase\" x=\"" << F2(width - kXpad - 16) << "\" y=\""
      << kFontSize * 2 << "\">ic</text>\n";
  svg << "<text id=\"matched\" x=\"" << F2(width - kXpad - 100) << "\" y=\""
      << height - kYpad2 / 2 << "\"> </text>\n";

  // frames
  svg << "<g id=\"frames\">\n";
  for (const auto& f : frames) {
    double x1 = kXpad + f.stime * width_per_time;
    double x2 = kXpad + f.etime * width_per_time;
    int y1 = height - kYpad2 - (f.depth + 1) * kFrameHeight + kFramePad;
    int y2 = height - kYpad2 - f.depth * kFrameHeight;
    uint64_t samples = f.etime - f.stime;

    std::string info;
    if (f.depth == 0) {
      info = "all (" + Commas(samples) + ' ' + options.countname + ", 100%)";
    } else {
      info = EscapeXml(f.func, true) + " (" + Commas(samples) + ' ' +
             options.countname + ", " +
             F2(100.0 * samples / total) + "%)";
    }

    svg << "<g><title>" << info << "</title><rect x=\"" << F1(x1) << "\" y=\""
        << y1 << "\" width=\"" << F1(x2 - x1) << "\" height=\""
        << F1(y2 - y1) << "\" fill=\"" << Color(options.palette, f.func)
        << "\" rx=\"2\" ry=\"2\"/>";

    int chars = static_cast<int>((x2 - x1) / (kFontSize * kFontWidth));
    std::string label;
    if (chars >= 3) {
      label = f.func.substr(0, chars);
      if (static_cast<int>(f.func.size()) > chars && label.size() >= 2) {
        label.replace(label.size() - 2, 2, "..");
      }
      label = EscapeXml(label, false);
    }
    svg << "<text x=\"" << F2(x1 + 3) << "\" y=\""
        << F1(3 + (y1 + y2) / 2.0) << "\">" << label << "</text></g>\n";
  }
  svg << "</g>\n</svg>\n";

  return static_cast<bool>(svg);
}

}  // namespace flamegraph
}  // namespace cache
}  // namespace dingofs
