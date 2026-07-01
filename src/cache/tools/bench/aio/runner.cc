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

#include "cache/tools/bench/aio/runner.h"

#include <bthread/bthread.h>
#include <butil/time.h>
#include <fcntl.h>
#include <glog/logging.h>
#include <unistd.h>

#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <random>

#include "cache/local/aio.h"
#include "cache/tools/bench/common/format.h"

namespace dingofs {
namespace cache {
namespace bench {
namespace aio {

namespace {
constexpr uint64_t kBufAlign = 4096;
constexpr uint64_t kPrepChunk = 1 * 1024 * 1024;

ConsoleReporter::HeaderRows BuildHeader(const Options& o) {
  ConsoleReporter::HeaderRows h;
  h.emplace_back("dir", o.dir);
  h.emplace_back("pattern", RwName(o.rw));
  if (o.rw == Rw::kRandRw) {
    h.emplace_back("rwmixread", std::to_string(o.rwmixread) + "%");
  }
  h.emplace_back("block size", FormatBytes(o.bs));
  h.emplace_back("iodepth", std::to_string(o.iodepth));
  h.emplace_back("threads", std::to_string(o.threads));
  h.emplace_back("files",
                 std::to_string(o.nrfiles) + " x " + FormatBytes(o.filesize));
  h.emplace_back("O_DIRECT", o.direct ? "yes" : "no");
  h.emplace_back("fixed buffers", o.fixed ? "yes" : "no");
  if (o.runtime_s > 0) h.emplace_back("runtime", std::to_string(o.runtime_s) + "s");
  if (o.io_size > 0) h.emplace_back("io_size", FormatBytes(o.io_size));
  if (o.warmup_s > 0) h.emplace_back("warmup", std::to_string(o.warmup_s) + "s");
  return h;
}
}  // namespace

Runner::Runner(Options options)
    : options_(options),
      stats_(std::make_unique<Stats>(options_.threads)),
      reporter_(std::make_unique<ConsoleReporter>(
          "DingoFS cb aio (io_uring/AioQueue)", BuildHeader(options_),
          options_.report_interval_s, options_.warmup_s, stats_.get())),
      profiler_(options_.profile) {}

Runner::~Runner() { Shutdown(); }

std::string Runner::FilePath(uint32_t index) const {
  return options_.dir + "/cb_aio." + std::to_string(index) + ".dat";
}

Status Runner::AllocBuffers() {
  // One buffer (and one registered fixed-buffer slot) per worker: each worker
  // has a single i/o in flight, so the concurrency -- and the buffer count --
  // is --threads, independent of the AioQueue ring depth (--iodepth).
  buffers_.assign(options_.threads, nullptr);
  for (uint32_t i = 0; i < options_.threads; ++i) {
    void* p = nullptr;
    if (posix_memalign(&p, kBufAlign, options_.bs) != 0 || p == nullptr) {
      return Status::Internal("posix_memalign failed");
    }
    std::memset(p, 0, options_.bs);
    buffers_[i] = static_cast<char*>(p);
  }
  if (options_.fixed) {
    fixed_iovecs_.resize(options_.threads);
    for (uint32_t i = 0; i < options_.threads; ++i) {
      fixed_iovecs_[i] = iovec{buffers_[i], options_.bs};
    }
  }
  return Status::OK();
}

Status Runner::OpenFiles() {
  int flags = O_RDWR | O_CREAT | (options_.direct ? O_DIRECT : 0);
  fds_.assign(options_.nrfiles, -1);
  for (uint32_t i = 0; i < options_.nrfiles; ++i) {
    int fd = ::open(FilePath(i).c_str(), flags, 0644);
    if (fd < 0) {
      return Status::Internal("open " + FilePath(i) + " failed: " +
                              std::strerror(errno));
    }
    if (::ftruncate(fd, static_cast<off_t>(options_.filesize)) != 0) {
      ::close(fd);
      return Status::Internal("ftruncate failed: " +
                              std::string(std::strerror(errno)));
    }
    fds_[i] = fd;
  }
  return Status::OK();
}

Status Runner::PrepFiles() {
  std::cout << "preparing " << options_.nrfiles << " file(s), "
            << FormatBytes(options_.filesize) << " each ...\n"
            << std::flush;
  std::vector<char> chunk(kPrepChunk, 0xA5);
  for (uint32_t i = 0; i < options_.nrfiles; ++i) {
    int fd = ::open(FilePath(i).c_str(), O_WRONLY, 0644);
    if (fd < 0) {
      return Status::Internal("open(prep) failed: " +
                              std::string(std::strerror(errno)));
    }
    uint64_t written = 0;
    while (written < options_.filesize) {
      const size_t n = static_cast<size_t>(
          std::min<uint64_t>(kPrepChunk, options_.filesize - written));
      const ssize_t rc = ::pwrite(fd, chunk.data(), n,
                                  static_cast<off_t>(written));
      if (rc < 0) {
        ::close(fd);
        return Status::Internal("pwrite(prep) failed: " +
                                std::string(std::strerror(errno)));
      }
      written += static_cast<uint64_t>(rc);
    }
    ::fsync(fd);
    ::close(fd);
  }
  return Status::OK();
}

Status Runner::Init() {
  DINGOFS_RETURN_NOT_OK(AllocBuffers());
  DINGOFS_RETURN_NOT_OK(OpenFiles());
  if (options_.prep && options_.DoesRead()) {
    DINGOFS_RETURN_NOT_OK(PrepFiles());
  }
  queue_ = std::make_unique<AioQueue>(options_.fixed ? fixed_iovecs_
                                                     : std::vector<iovec>{});
  return queue_->Start();
}

void* Runner::GenEntry(void* arg) {
  auto* gen_arg = static_cast<GenArg*>(arg);
  gen_arg->self->RunWorker(gen_arg->id);
  return nullptr;
}

void Runner::RunWorker(uint32_t worker) {
  const uint32_t nrfiles = options_.nrfiles;
  const uint64_t bs = options_.bs;
  const uint64_t nblocks = options_.filesize / bs;
  const uint32_t threads = options_.threads;

  // Each worker is one closed-loop submitter: it keeps exactly ONE i/o in
  // flight (Submit + Wait), so the offered concurrency is --threads. --iodepth
  // only sizes the AioQueue/io_uring ring (FLAGS_iodepth), exactly like the
  // production cache uses it.
  int fd = fds_[worker % nrfiles];
  char* buffer = buffers_[worker];
  const int buf_index = options_.fixed ? static_cast<int>(worker) : -1;

  uint64_t stride = (threads / nrfiles) +
                    ((threads % nrfiles) > (worker % nrfiles) ? 1 : 0);
  if (stride == 0) stride = 1;
  uint64_t seq_block = worker / nrfiles;

  std::mt19937_64 rng(0x9E3779B97F4A7C15ULL ^
                      (static_cast<uint64_t>(worker + 1) * 0x100000001B3ULL));
  const bool random = options_.IsRandom();
  const bool always_read =
      options_.rw == Rw::kRead || options_.rw == Rw::kRandRead;
  const bool always_write =
      options_.rw == Rw::kWrite || options_.rw == Rw::kRandWrite;

  uint64_t l_submit = 0, l_wait = 0, l_ops = 0;  // per-worker, merged at exit
  for (;;) {
    if (stop_.load(std::memory_order_relaxed)) break;
    if (deadline_us_ != 0 && butil::gettimeofday_us() >= deadline_us_) break;
    if (options_.io_size != 0 &&
        bytes_issued_.fetch_add(bs, std::memory_order_relaxed) >=
            options_.io_size) {
      break;
    }

    uint64_t block;
    if (random) {
      block = rng() % nblocks;
    } else {
      block = seq_block % nblocks;
      seq_block += stride;
    }
    const off_t offset = static_cast<off_t>(block * bs);

    bool for_read = true;
    if (always_write) {
      for_read = false;
    } else if (!always_read) {
      for_read = (rng() % 100) < options_.rwmixread;
    }

    Aio aio(fd, offset, bs, buffer, buf_index, for_read);
    const uint64_t start_us = butil::gettimeofday_us();
    queue_->Submit(&aio);
    const uint64_t submitted_us = butil::gettimeofday_us();
    aio.Wait();
    const uint64_t done_us = butil::gettimeofday_us();
    stats_->Record(worker, bs, done_us - start_us, aio.Result().status.ok());
    l_submit += submitted_us - start_us;  // time in Submit()
    l_wait += done_us - submitted_us;     // time blocked in Wait()
    ++l_ops;
  }
  submit_sum_us_.fetch_add(l_submit, std::memory_order_relaxed);
  wait_sum_us_.fetch_add(l_wait, std::memory_order_relaxed);
  op_count_.fetch_add(l_ops, std::memory_order_relaxed);
}

Status Runner::Run() {
  auto status = Init();
  if (!status.ok()) {
    Shutdown();
    return status;
  }
  // Start capturing before the reporter prints its table, so the [flamegraph]
  // lines don't split the metrics output (perf -p follows the worker threads
  // spawned below). Covers both runtime and io_size modes.
  profiler_.Start();
  status = reporter_->Start();
  if (!status.ok()) {
    Shutdown();
    return status;
  }

  const uint64_t start_us = butil::gettimeofday_us();
  if (options_.runtime_s > 0) {
    deadline_us_ = start_us + (static_cast<uint64_t>(options_.warmup_s +
                                                     options_.runtime_s) *
                              1000000ULL);
  }

  std::vector<bthread_t> tids(options_.threads, 0);
  std::vector<GenArg> args(options_.threads);
  for (uint32_t i = 0; i < options_.threads; ++i) {
    args[i] = GenArg{this, i};
    if (bthread_start_background(&tids[i], nullptr, &Runner::GenEntry,
                                 &args[i]) != 0) {
      tids[i] = 0;
      RunWorker(i);
    }
  }

  if (options_.runtime_s > 0) {
    if (options_.warmup_s > 0) {
      ::sleep(options_.warmup_s);
      stats_->Reset();
      reporter_->ResetBaseline();
    }
    ::sleep(options_.runtime_s);
    stop_.store(true, std::memory_order_relaxed);
  }

  for (uint32_t i = 0; i < options_.threads; ++i) {
    if (tids[i] != 0) bthread_join(tids[i], nullptr);
  }

  profiler_.Stop();
  reporter_->Stop();

  const uint64_t ops = op_count_.load(std::memory_order_relaxed);
  if (ops > 0) {
    std::cout << "\nPer-op breakdown (closed loop)\n------------------------------\n";
    std::cout << "  " << std::left << std::setw(16) << "AioQueue.Submit()"
              << FormatLatencyUs(submit_sum_us_.load() / ops) << " avg\n";
    std::cout << "  " << std::left << std::setw(16) << "Aio.Wait()"
              << FormatLatencyUs(wait_sum_us_.load() / ops) << " avg\n";
  }

  Shutdown();
  profiler_.RenderAndServe();
  return Status::OK();
}

void Runner::Shutdown() {
  if (queue_ != nullptr) {
    auto status = queue_->Shutdown();
    if (!status.ok()) {
      LOG(ERROR) << "Shutdown AioQueue failed: " << status.ToString();
    }
    queue_.reset();
  }
  for (int fd : fds_) {
    if (fd >= 0) ::close(fd);
  }
  fds_.clear();
  for (char* buffer : buffers_) {
    free(buffer);
  }
  buffers_.clear();
  fixed_iovecs_.clear();
  if (!options_.keep) {
    for (uint32_t i = 0; i < options_.nrfiles; ++i) {
      ::unlink(FilePath(i).c_str());
    }
  }
}

}  // namespace aio
}  // namespace bench
}  // namespace cache
}  // namespace dingofs
