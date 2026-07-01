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

#include "cache/tools/bench/common/profiler.h"

#include "cache/tools/bench/common/file_server.h"
#include "cache/tools/flamegraph/flamegraph.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/prctl.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <cctype>
#include <csignal>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <set>
#include <sstream>
#include <string>
#include <vector>

// brpc's in-process lock-contention profiler. Not exported in any public
// header, so forward-declare it (cb already links brpc). Captures wall-clock
// time blocked on bthread/pthread mutexes, with stacks, no Server needed.
namespace bthread {
bool ContentionProfilerStart(const char* filename);
void ContentionProfilerStop();
}  // namespace bthread

namespace dingofs {
namespace cache {
namespace bench {

namespace {

// Bind to port 0 to let the kernel hand us a free ephemeral port, then close.
int PickFreePort() {
  int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) return 0;
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  addr.sin_port = 0;
  int port = 0;
  if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0) {
    socklen_t len = sizeof(addr);
    if (::getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &len) == 0) {
      port = ntohs(addr.sin_port);
    }
  }
  ::close(fd);
  return port;
}

// Best-effort primary IP: a UDP "connect" sends nothing but makes the kernel
// pick the outbound interface, so getsockname yields the reachable address.
std::string PrimaryIp() {
  int fd = ::socket(AF_INET, SOCK_DGRAM, 0);
  if (fd < 0) return "127.0.0.1";
  sockaddr_in remote{};
  remote.sin_family = AF_INET;
  remote.sin_port = htons(53);
  ::inet_pton(AF_INET, "8.8.8.8", &remote.sin_addr);
  std::string ip = "127.0.0.1";
  if (::connect(fd, reinterpret_cast<sockaddr*>(&remote), sizeof(remote)) == 0) {
    sockaddr_in local{};
    socklen_t len = sizeof(local);
    if (::getsockname(fd, reinterpret_cast<sockaddr*>(&local), &len) == 0) {
      char buf[INET_ADDRSTRLEN];
      if (::inet_ntop(AF_INET, &local.sin_addr, buf, sizeof(buf)) != nullptr) {
        ip = buf;
      }
    }
  }
  ::close(fd);
  return ip;
}

// Locate an executable in PATH; returns full path or "".
std::string Which(const std::string& name) {
  const char* path = ::getenv("PATH");
  if (path == nullptr) return "";
  std::stringstream ss(path);
  std::string dir;
  while (std::getline(ss, dir, ':')) {
    if (dir.empty()) continue;
    std::string cand = dir + "/" + name;
    if (::access(cand.c_str(), X_OK) == 0) return cand;
  }
  return "";
}

// offcputime ships in different places per distro: in PATH on Debian/Ubuntu
// (offcputime-bpfcc), but /usr/share/bcc/tools/offcputime on RHEL/Fedora (NOT in
// PATH, no -bpfcc suffix). Probe all of them so no PATH setup is needed.
std::string ResolveOffcputime() {
  for (const char* name : {"offcputime-bpfcc", "offcputime"}) {
    std::string r = Which(name);
    if (!r.empty()) return r;
  }
  for (const char* path :
       {"/usr/share/bcc/tools/offcputime", "/usr/sbin/offcputime-bpfcc"}) {
    if (::access(path, X_OK) == 0) return path;
  }
  return "";
}

// addr2line is normally in PATH (binutils), but probe well-known absolute
// locations too so a stripped/odd PATH (e.g. only a gcc-toolset addr2line is on
// PATH while /usr/bin/addr2line exists) still resolves it.
std::string ResolveAddr2line() {
  std::string r = Which("addr2line");
  if (!r.empty()) return r;
  for (const char* path : {"/usr/bin/addr2line", "/bin/addr2line"}) {
    if (::access(path, X_OK) == 0) return path;
  }
  return "";
}

bool FileExists(const std::string& path) {
  return ::access(path.c_str(), R_OK) == 0;
}

bool FileNonEmpty(const std::string& path) {
  struct stat st{};
  return ::stat(path.c_str(), &st) == 0 && st.st_size > 0;
}

// Run perf script on the recorded data and fold it into native folded stacks.
// Uses /bin/sh only as a pipe to a file (always present, not a runtime dep we
// carry); the perf-script text is transient and removed afterwards.
bool PerfDataToFolded(const std::string& perf_bin, const std::string& data,
                      const std::string& folded) {
  const std::string txt = data + ".script.txt";
  const std::string cmd =
      perf_bin + " script -i '" + data + "' > '" + txt + "' 2>/dev/null";
  std::system(cmd.c_str());  // NOLINT(cert-env33-c)
  std::ifstream in(txt);
  bool ok = in && flamegraph::CollapsePerfScript(in, folded);
  in.close();
  ::unlink(txt.c_str());
  return ok;
}

// Parse --profile_mode into on/off/lock. Accepts comma-separated tokens
// (on_cpu|off_cpu|lock) plus aliases both(=on+off) and all(=all three).
bool ParseMode(const std::string& mode, bool* on, bool* off, bool* lock) {
  *on = *off = *lock = false;
  std::stringstream ss(mode);
  std::string tok;
  while (std::getline(ss, tok, ',')) {
    if (tok.empty()) continue;
    if (tok == "on_cpu") {
      *on = true;
    } else if (tok == "off_cpu") {
      *off = true;
    } else if (tok == "lock") {
      *lock = true;
    } else if (tok == "both") {
      *on = *off = true;
    } else if (tok == "all") {
      *on = *off = *lock = true;
    } else {
      return false;
    }
  }
  return *on || *off || *lock;
}

uint64_t FoldedTotalUs(const std::string& folded) {
  std::ifstream in(folded);
  std::string line;
  uint64_t total = 0;
  while (std::getline(in, line)) {
    size_t sp = line.rfind(' ');
    if (sp != std::string::npos) {
      total += std::strtoull(line.c_str() + sp + 1, nullptr, 10);
    }
  }
  return total;
}

// --- lock-contention symbolization (brpc pprof-contention -> folded) --------

struct MapSeg {
  uint64_t start, end, offset;
  std::string path;
};

std::vector<MapSeg> ReadSelfMaps() {
  std::vector<MapSeg> segs;
  std::ifstream in("/proc/self/maps");
  std::string line;
  while (std::getline(in, line)) {
    uint64_t start = 0, end = 0, offset = 0;
    char perms[8] = {0};
    int path_pos = 0;
    if (std::sscanf(line.c_str(), "%lx-%lx %7s %lx %*s %*s %n", &start, &end,
                    perms, &offset, &path_pos) < 4) {
      continue;
    }
    if (perms[2] != 'x') continue;  // executable mappings only
    std::string path = path_pos > 0 ? line.substr(path_pos) : "";
    while (!path.empty() && path.front() == ' ') path.erase(path.begin());
    if (path.empty() || path[0] != '/') continue;  // skip [vdso], anon
    segs.push_back({start, end, offset, path});
  }
  return segs;
}

// ELF e_type: 2 = ET_EXEC (no PIE, use addr as-is), 3 = ET_DYN (PIE/.so).
int ElfType(const std::string& path) {
  std::ifstream f(path, std::ios::binary);
  char hdr[20] = {0};
  if (!f.read(hdr, sizeof(hdr))) return 0;
  if (hdr[0] != 0x7f || hdr[1] != 'E') return 0;
  return static_cast<unsigned char>(hdr[16]) |
         (static_cast<unsigned char>(hdr[17]) << 8);
}

std::string TrimLine(const char* s) {
  std::string r(s);
  while (!r.empty() && (r.back() == '\n' || r.back() == '\r' ||
                        r.back() == ' ' || r.back() == '\t')) {
    r.pop_back();
  }
  return r;
}

// Symbolize raw runtime addresses to function names via addr2line. Empty map
// if addr2line is missing.
std::map<uint64_t, std::string> SymbolizeAddrs(const std::set<uint64_t>& addrs) {
  std::map<uint64_t, std::string> out;
  const std::string a2l = ResolveAddr2line();
  if (a2l.empty() || addrs.empty()) return out;

  const std::vector<MapSeg> segs = ReadSelfMaps();
  std::map<std::string, int> etype;  // module -> ELF e_type
  for (const auto& s : segs) {
    if (etype.find(s.path) == etype.end()) etype[s.path] = ElfType(s.path);
  }

  // group addresses by module, computing the ELF vaddr addr2line expects. For
  // ET_EXEC the runtime addr is the vaddr; for ET_DYN (PIE/.so) the bias is
  // per-segment (start-offset), which handles modules whose exec segment sits
  // at a file offset (e.g. libc) -- a per-module base would mis-symbolize those.
  std::map<std::string, std::vector<std::pair<uint64_t, uint64_t>>> by_mod;
  for (uint64_t addr : addrs) {
    const MapSeg* seg = nullptr;
    for (const auto& s : segs) {
      if (addr >= s.start && addr < s.end) {
        seg = &s;
        break;
      }
    }
    if (seg == nullptr) continue;
    uint64_t vaddr =
        etype[seg->path] == 2 ? addr : addr - seg->start + seg->offset;
    by_mod[seg->path].push_back({addr, vaddr});
  }

  for (const auto& [path, list] : by_mod) {
    std::string cmd = a2l + " -f -C -e '" + path + "'";
    for (const auto& pr : list) {
      char hex[32];
      std::snprintf(hex, sizeof(hex), " 0x%lx", pr.second);
      cmd += hex;
    }
    cmd += " 2>/dev/null";
    FILE* fp = ::popen(cmd.c_str(), "r");
    if (fp == nullptr) continue;
    char buf[4096];
    // addr2line -f emits two lines per address: function, then file:line.
    for (size_t i = 0; i < list.size() && std::fgets(buf, sizeof(buf), fp);
         ++i) {
      std::string func = TrimLine(buf);
      if (std::fgets(buf, sizeof(buf), fp) == nullptr) {/* file:line */}
      if (func.empty() || func == "??") func = "[unknown]";
      std::replace(func.begin(), func.end(), ';', ':');
      out[list[i].first] = func;
    }
    ::pclose(fp);
  }
  return out;
}

// Convert a brpc contention profile to folded stacks (count = microseconds
// blocked). Returns false if addr2line is missing or there are no samples.
bool ContentionToFolded(const std::string& prof, const std::string& folded) {
  std::ifstream in(prof);
  if (!in) return false;

  struct Sample {
    uint64_t dur_ns;
    std::vector<uint64_t> addrs;  // leaf-first (pprof order)
  };
  std::vector<Sample> samples;
  std::set<uint64_t> all_addrs;
  std::string line;
  while (std::getline(in, line)) {
    if (line.empty() ||
        std::isdigit(static_cast<unsigned char>(line[0])) == 0) {
      // header lines, or the trailing /proc maps section -> stop at a maps line
      if (line.rfind("---", 0) == 0 && line.find("contention") == std::string::npos) {
        break;
      }
      continue;
    }
    size_t at = line.find(" @ ");
    if (at == std::string::npos) continue;
    Sample s;
    s.dur_ns = std::strtoull(line.c_str(), nullptr, 10);
    std::stringstream ss(line.substr(at + 3));
    std::string a;
    while (ss >> a) {
      uint64_t addr = std::strtoull(a.c_str(), nullptr, 16);
      if (addr != 0) {
        s.addrs.push_back(addr);
        all_addrs.insert(addr);
      }
    }
    if (!s.addrs.empty()) samples.push_back(std::move(s));
  }
  if (samples.empty()) return false;

  const std::map<uint64_t, std::string> sym = SymbolizeAddrs(all_addrs);
  std::map<std::string, uint64_t> stacks;  // folded stack -> total ns
  for (const auto& s : samples) {
    std::string stack;
    for (auto it = s.addrs.rbegin(); it != s.addrs.rend(); ++it) {  // root-first
      if (!stack.empty()) stack += ';';
      auto f = sym.find(*it);
      stack += (f != sym.end() && !f->second.empty()) ? f->second : "[unknown]";
    }
    stacks[stack] += s.dur_ns;
  }

  std::ofstream out(folded);
  if (!out) return false;
  for (const auto& [stack, ns] : stacks) {
    uint64_t us = ns / 1000;
    if (us > 0) out << stack << ' ' << us << '\n';
  }
  return static_cast<bool>(out);
}

}  // namespace

void RegisterProfileFlags(FlagSet* fs, ProfileOptions* o) {
  fs->Switch("flamegraph", &o->flamegraph,
             "capture on-cpu/off-cpu flame graphs and serve them over http");
  fs->Str("profile_mode", &o->mode,
          "flame graphs: on_cpu|off_cpu|lock|both(=on+off)|all; comma-combinable");
  fs->U32("profile_freq", &o->freq, "on-cpu perf sampling frequency (Hz)");
  fs->Str("profile_dir", &o->dir,
          "flame graph output dir; empty = /tmp/cb-flame-<pid>");
  fs->U32("profile_port", &o->port, "http server port; 0 = auto-pick free port");
}

std::string ValidateProfile(const ProfileOptions& o) {
  if (!o.flamegraph) return "";
  bool on = false, off = false, lock = false;
  if (!ParseMode(o.mode, &on, &off, &lock)) {
    return "profile_mode must be on_cpu|off_cpu|lock|both|all (comma-ok): " +
           o.mode;
  }
  if (o.freq == 0 || o.freq > 10000) return "profile_freq must be in [1, 10000]";
  if (o.port > 65535) return "profile_port must be <= 65535";

  // Fail fast: a requested mode whose tool is missing aborts the run with an
  // install hint, rather than silently skipping.
  if (on && Which("perf").empty()) {
    return "profile_mode=on_cpu needs `perf` -- install it: dnf install perf";
  }
  if ((on || lock) && ResolveAddr2line().empty()) {
    return std::string("profile_mode=") + (on ? "on_cpu" : "lock") +
           " needs `addr2line` to symbolize -- install it: dnf install binutils";
  }
  if (off && ResolveOffcputime().empty()) {
    return "profile_mode=off_cpu needs bcc `offcputime` -- install it: "
           "dnf install bcc-tools";
  }
  if (off && ::geteuid() != 0) {
    return "profile_mode=off_cpu (eBPF offcputime) needs root -- run cb with sudo";
  }
  return "";
}

pid_t Profiler::Spawn(const std::vector<std::string>& args,
                      const std::string& out_path,
                      const std::string& err_path) {
  // Build argv BEFORE fork: nothing between fork and exec may allocate.
  std::vector<char*> argv;
  argv.reserve(args.size() + 1);
  for (const auto& a : args) argv.push_back(const_cast<char*>(a.c_str()));
  argv.push_back(nullptr);

  int out_fd = -1;
  if (!out_path.empty()) {
    out_fd = ::open(out_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (out_fd < 0) return -1;  // refuse to spawn without the intended redirect
  }
  int err_fd = -1;
  if (!err_path.empty()) {
    err_fd = (err_path == out_path)
                 ? out_fd
                 : ::open(err_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
  }

  pid_t pid = ::fork();
  if (pid < 0) {
    if (out_fd >= 0) ::close(out_fd);
    if (err_fd >= 0 && err_fd != out_fd) ::close(err_fd);
    return -1;
  }
  if (pid == 0) {
    // Child: async-signal-safe calls only until exec.
    ::prctl(PR_SET_PDEATHSIG, SIGINT);  // die (and flush) if cb's main thread does
    if (out_fd >= 0) ::dup2(out_fd, STDOUT_FILENO);
    if (err_fd >= 0) ::dup2(err_fd, STDERR_FILENO);
    ::execvp(argv[0], argv.data());
    ::_exit(127);
  }
  if (out_fd >= 0) ::close(out_fd);
  if (err_fd >= 0 && err_fd != out_fd) ::close(err_fd);
  return pid;
}

void Profiler::Start() {
  if (!options_.flamegraph || started_) return;

  ParseMode(options_.mode, &want_on_cpu_, &want_off_cpu_, &want_lock_);

  const std::string pid = std::to_string(::getpid());
  out_dir_ = options_.dir.empty() ? "/tmp/cb-flame-" + pid : options_.dir;
  std::error_code ec;
  std::filesystem::create_directories(out_dir_, ec);
  if (ec) {
    std::cerr << "[flamegraph] cannot create " << out_dir_ << ": " << ec.message()
              << "; profiling disabled\n";
    return;
  }

  if (want_on_cpu_) {
    perf_bin_ = Which("perf");
    if (perf_bin_.empty()) {
      std::cerr << "[flamegraph] perf not found in PATH; skipping on-cpu "
                   "(install: dnf install perf)\n";
      want_on_cpu_ = false;
    } else {
      const std::string log = out_dir_ + "/on_cpu.log";
      on_cpu_pid_ = Spawn({perf_bin_, "record", "-F", std::to_string(options_.freq),
                           "--call-graph", "dwarf,32768", "-p", pid, "-o",
                           out_dir_ + "/on_cpu.perf.data"},
                          log, log);
      if (on_cpu_pid_ < 0) {
        std::cerr << "[flamegraph] failed to launch perf; skipping on-cpu\n";
        want_on_cpu_ = false;
      }
    }
  }

  if (want_off_cpu_) {
    offcputime_bin_ = ResolveOffcputime();
    if (offcputime_bin_.empty()) {
      std::cerr << "[flamegraph] offcputime not found; skipping off-cpu "
                   "(install: dnf install bcc-tools)\n";
      want_off_cpu_ = false;
    } else {
      // Folded stacks go to stdout (-> off_cpu.folded); a big duration is cut
      // short by the SIGINT in Stop(), which makes offcputime print and exit.
      off_cpu_pid_ = Spawn({offcputime_bin_, "-d", "-f", "-p", pid, "99999"},
                           out_dir_ + "/off_cpu.folded", out_dir_ + "/off_cpu.log");
      if (off_cpu_pid_ < 0) {
        std::cerr << "[flamegraph] failed to launch offcputime; skipping off-cpu\n";
        want_off_cpu_ = false;
      }
    }
  }

  if (want_lock_) {
    // brpc in-process contention profiler -- no perf/bcc/root needed.
    lock_active_ =
        bthread::ContentionProfilerStart((out_dir_ + "/contention.prof").c_str());
    if (!lock_active_) {
      std::cerr << "[flamegraph] could not start lock-contention profiler "
                   "(another profiler already running)\n";
    }
  }

  started_ = on_cpu_pid_ > 0 || off_cpu_pid_ > 0 || lock_active_;
  if (started_) {
    std::cout << "[flamegraph] capturing "
              << (want_on_cpu_ ? "on-cpu " : "")
              << (want_off_cpu_ ? "off-cpu " : "")
              << (lock_active_ ? "lock " : "") << "-> " << out_dir_ << '\n'
              << std::flush;
  }
}

void Profiler::Stop() {
  if (!started_) return;
  // Signal both first so they wind down together, then reap (no zombies before
  // the exec in RenderAndServe).
  if (on_cpu_pid_ > 0) ::kill(on_cpu_pid_, SIGINT);
  if (off_cpu_pid_ > 0) ::kill(off_cpu_pid_, SIGINT);
  int status = 0;
  if (on_cpu_pid_ > 0) {
    ::waitpid(on_cpu_pid_, &status, 0);
    on_cpu_pid_ = -1;
  }
  if (off_cpu_pid_ > 0) {
    ::waitpid(off_cpu_pid_, &status, 0);
    off_cpu_pid_ = -1;
  }
  if (lock_active_) {
    bthread::ContentionProfilerStop();  // flushes contention.prof
    lock_active_ = false;
  }
}

void Profiler::RenderAndServe() {
  if (!started_) return;

  const std::string on_cpu_data = out_dir_ + "/on_cpu.perf.data";
  const std::string on_cpu_folded = out_dir_ + "/on_cpu.folded";
  const std::string off_cpu_folded = out_dir_ + "/off_cpu.folded";
  const std::string on_cpu_svg = out_dir_ + "/on_cpu.svg";
  const std::string off_cpu_svg = out_dir_ + "/off_cpu.svg";
  const std::string contention_prof = out_dir_ + "/contention.prof";
  const std::string lock_folded = out_dir_ + "/lock.folded";
  const std::string lock_svg = out_dir_ + "/lock.svg";

  std::cout << '\n';

  // on-cpu: perf.data -> folded (native) -> svg (native).
  if (want_on_cpu_ && FileExists(on_cpu_data)) {
    PerfDataToFolded(perf_bin_, on_cpu_data, on_cpu_folded);
    flamegraph::FlameOptions o;
    o.title = "cb on-cpu";
    o.palette = flamegraph::Palette::kHot;
    if (FileNonEmpty(on_cpu_folded) &&
        flamegraph::RenderFlameSvg(on_cpu_folded, on_cpu_svg, o)) {
      std::cout << "[flamegraph] on-cpu  : " << on_cpu_svg << '\n';
    }
  }

  // off-cpu: offcputime already wrote folded stacks; just render (native).
  if (want_off_cpu_ && FileNonEmpty(off_cpu_folded)) {
    flamegraph::FlameOptions o;
    o.title = "cb off-cpu";
    o.countname = "us";
    o.palette = flamegraph::Palette::kIo;
    if (flamegraph::RenderFlameSvg(off_cpu_folded, off_cpu_svg, o)) {
      // counts are microseconds: report the absolute total (summed over threads)
      const uint64_t total_us = FoldedTotalUs(off_cpu_folded);
      std::cout << "[flamegraph] off-cpu : " << off_cpu_svg << "  (total blocked ≈ "
                << total_us / 1000 << " ms across threads; hover a frame for its us)\n";
    }
  } else if (want_off_cpu_) {
    std::cout << "[flamegraph] off-cpu : no off-cpu samples captured "
                 "(workload was CPU-bound, or offcputime produced no stacks)\n";
  }

  // lock contention: brpc profile -> symbolized folded (us) -> svg (native).
  if (want_lock_ && FileNonEmpty(contention_prof)) {
    if (ResolveAddr2line().empty()) {
      std::cout << "[flamegraph] lock    : kept " << contention_prof
                << " (addr2line not on PATH; `dnf install binutils` then re-run, "
                   "or render later: addr2line-symbolize this file)\n";
    } else if (ContentionToFolded(contention_prof, lock_folded) &&
               FileNonEmpty(lock_folded)) {
      flamegraph::FlameOptions o;
      o.title = "cb lock-contention";
      o.countname = "us";
      o.palette = flamegraph::Palette::kHot;
      if (flamegraph::RenderFlameSvg(lock_folded, lock_svg, o)) {
        const uint64_t total_us = FoldedTotalUs(lock_folded);
        std::cout << "[flamegraph] lock    : " << lock_svg << "  (total on locks ≈ "
                  << total_us / 1000 << " ms; hover a frame for its us)\n";
      }
    } else {
      // addr2line is present, but the profile has no usable samples: the run
      // simply took few/no contended mutexes (e.g. cb aio uses condvars, not
      // mutexes). Use store/client with higher concurrency to see contention.
      std::cout << "[flamegraph] lock    : no lock contention captured "
                   "(workload took few/no contended mutexes; try store/client "
                   "with higher --jobs/--threads)\n";
    }
  } else if (want_lock_) {
    std::cout << "[flamegraph] lock    : no lock contention captured "
                 "(workload took no contended mutexes)\n";
  }

  // index.html linking whatever artifacts exist.
  {
    std::ofstream html(out_dir_ + "/index.html");
    html << "<!doctype html><html><head><meta charset=\"utf-8\">"
            "<title>cb flame graphs</title><style>body{font-family:sans-serif;"
            "margin:2rem}a{display:block;margin:.4rem 0}</style></head><body>"
            "<h1>cb flame graphs</h1><ul>";
    if (FileNonEmpty(on_cpu_svg)) {
      html << "<li><a href=\"on_cpu.svg\">on-cpu</a> &mdash; where CPU time goes"
              "</li>";
    }
    if (FileNonEmpty(off_cpu_svg)) {
      html << "<li><a href=\"off_cpu.svg\">off-cpu</a> &mdash; where threads "
              "block / wait, absolute us (io &amp; locks &amp; ctx-switch)</li>";
    }
    if (FileNonEmpty(lock_svg)) {
      html << "<li><a href=\"lock.svg\">lock-contention</a> &mdash; absolute us "
              "blocked on mutexes, per call path</li>";
    }
    html << "</ul><p>raw folded stacks: ";
    if (FileNonEmpty(on_cpu_folded)) html << "<a href=\"on_cpu.folded\">on_cpu</a> ";
    if (FileNonEmpty(off_cpu_folded)) html << "<a href=\"off_cpu.folded\">off_cpu</a> ";
    if (FileNonEmpty(lock_folded)) html << "<a href=\"lock.folded\">lock</a>";
    html << "</p></body></html>";
  }

  const int port = options_.port != 0 ? static_cast<int>(options_.port)
                                       : PickFreePort();
  if (port == 0) {
    std::cout << "[flamegraph] no free port; open the files under " << out_dir_
              << " manually\n"
              << std::flush;
    return;
  }
  std::cout << "[flamegraph] serving : http://" << PrimaryIp() << ':' << port
            << "/   (Ctrl-C to stop)\n"
            << std::flush;

  ServeDirectory(out_dir_, port);  // blocks until Ctrl-C
}

}  // namespace bench
}  // namespace cache
}  // namespace dingofs
