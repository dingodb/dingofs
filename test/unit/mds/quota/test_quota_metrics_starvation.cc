// Copyright (c) 2024 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Regression for brpc#3470: FsQuotaMetric getters may wait on Quota's
// BthreadRWLock. The callback must run outside bvar's VarMap pthread mutex;
// otherwise later metric scrapes pin every worker and starve mutations.
// The deadlocking implementation is exercised in a child process so the
// parent can enforce a real timeout and kill it safely.

#include <poll.h>
#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>

#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <string>
#include <thread>
#include <vector>

#include "bthread/bthread.h"
#include "common/metrics/mds/quota_metrics.h"
#include "gtest/gtest.h"
#include "mds/quota/quota.h"

namespace dingofs {
namespace mds {
namespace unit_test {
namespace {

using quota::Quota;

constexpr int kWorkers = 8;
constexpr int kScrapesPerWave = 16;
constexpr int kMutationCount = 12;
constexpr int kWatchdogMs = 15'000;
constexpr char kPrefix[] = "fs_quota_starvation_test";
constexpr char kMetricName[] =
    "fs_quota_starvation_test_bytes_usage_ratio";

QuotaEntry MakeQuota() {
  QuotaEntry quota;
  quota.set_max_bytes(100);
  quota.set_max_inodes(1000);
  quota.set_uuid("starvation-test");
  quota.set_version(1);
  return quota;
}

struct MetricSource {
  static int64_t UsedBytes(void* arg) {
    return static_cast<MetricSource*>(arg)->quota->GetQuota().used_bytes();
  }
  static int64_t UsedInodes(void* arg) {
    return static_cast<MetricSource*>(arg)->quota->GetQuota().used_inodes();
  }
  static int64_t MaxBytes(void* arg) {
    return static_cast<MetricSource*>(arg)->quota->GetQuota().max_bytes();
  }
  static int64_t MaxInodes(void* arg) {
    return static_cast<MetricSource*>(arg)->quota->GetQuota().max_inodes();
  }

  Quota* quota;
};

struct Scenario {
  Quota quota{1, 10, MakeQuota()};
  MetricSource source{&quota};
  metrics::mds::FsQuotaMetric metric{
      kPrefix, &source, MetricSource::UsedBytes, MetricSource::UsedInodes,
      MetricSource::MaxBytes, MetricSource::MaxInodes};
  std::atomic<bool> stop{false};
  std::atomic<int> scrape_start_error{0};

  Scenario() {
    for (auto& ran : mutation_ran) {
      ran.store(false);
    }
  }
  std::array<std::atomic<bool>, kMutationCount> mutation_ran{};
  std::vector<bthread_t> scrape_tids;
};

void* Writer(void* arg) {
  auto* scenario = static_cast<Scenario*>(arg);
  while (!scenario->stop.load(std::memory_order_relaxed)) {
    scenario->quota.UpdateUsage(1, 0, "starvation-test");
  }
  return nullptr;
}

void* Scrape(void*) {
  bvar::Variable::describe_exposed(kMetricName);
  return nullptr;
}

void* LaunchScrapes(void* arg) {
  auto* scenario = static_cast<Scenario*>(arg);
  while (!scenario->stop.load(std::memory_order_relaxed)) {
    for (int i = 0; i < kScrapesPerWave; ++i) {
      bthread_t tid;
      const int rc = bthread_start_urgent(&tid, nullptr, Scrape, nullptr);
      if (rc != 0) {
        scenario->scrape_start_error.store(rc);
        return nullptr;
      }
      scenario->scrape_tids.push_back(tid);
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
  return nullptr;
}

void* Mutation(void* arg) {
  static_cast<std::atomic<bool>*>(arg)->store(true);
  return nullptr;
}

std::string RunScenario(bool with_writer, bool with_scrapes,
                        int mutation_count) {
  if (bthread_setconcurrency(kWorkers) != 0) {
    return "bthread_setconcurrency failed";
  }

  // Heap-allocated and intentionally leaked: on unpatched brpc the
  // metric destructor would try to take the poisoned VarMap mutex.
  Scenario* scenario = new Scenario;
  if (bvar::Variable::describe_exposed(kMetricName).empty()) {
    return "quota metric is not exposed";
  }

  bthread_t writer_tid = 0;
  pthread_t scrape_launcher_tid = 0;
  std::vector<bthread_t> mutation_tids;

  if (with_writer &&
      bthread_start_urgent(&writer_tid, nullptr, Writer, scenario) != 0) {
    return "writer start failed";
  }
  if (with_scrapes &&
      pthread_create(&scrape_launcher_tid, nullptr, LaunchScrapes,
                     scenario) != 0) {
    return "scrape launcher start failed";
  }

  // Let the scrape/writer convoy form before probing the scheduler: on
  // unpatched brpc the first parked scrape holds the VarMap mutex and
  // waiters pin the workers within a few waves.
  std::this_thread::sleep_for(std::chrono::milliseconds(500));


  for (int i = 0; i < mutation_count; ++i) {
    bthread_t tid;
    if (bthread_start_urgent(&tid, nullptr, Mutation,
                             &scenario->mutation_ran[i]) != 0) {
      return "mutation start failed";
    }
    mutation_tids.push_back(tid);

    for (int waited_ms = 0;
         waited_ms < 2000 && !scenario->mutation_ran[i].load();
         waited_ms += 5) {
      std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    if (!scenario->mutation_ran[i].load()) {
      return "mutation " + std::to_string(i) + "/" +
             std::to_string(mutation_count) + " was never scheduled";
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(250));
  }

  scenario->stop.store(true);
  if (with_scrapes) {
    pthread_join(scrape_launcher_tid, nullptr);
  }
  void* retval = nullptr;
  if (with_writer) {
    bthread_join(writer_tid, &retval);
  }
  for (bthread_t tid : scenario->scrape_tids) {
    bthread_join(tid, &retval);
  }
  for (bthread_t tid : mutation_tids) {
    bthread_join(tid, &retval);
  }
  if (scenario->scrape_start_error.load() != 0) {
    return "scrape start failed";
  }
  return "OK";
}

std::string RunInChild(bool with_writer, bool with_scrapes,
                       int mutation_count) {
  int fds[2];
  if (pipe(fds) != 0) {
    return "pipe failed";
  }

  const pid_t pid = fork();
  if (pid < 0) {
    close(fds[0]);
    close(fds[1]);
    return "fork failed";
  }
  if (pid == 0) {
    close(fds[0]);
    const std::string verdict =
        RunScenario(with_writer, with_scrapes, mutation_count);
    (void)write(fds[1], verdict.data(), verdict.size());
    _exit(verdict == "OK" ? 0 : 1);
  }

  close(fds[1]);
  struct pollfd pfd{fds[0], POLLIN, 0};
  const int poll_rc = poll(&pfd, 1, kWatchdogMs);
  if (poll_rc <= 0) {
    kill(pid, SIGKILL);
    waitpid(pid, nullptr, 0);
    close(fds[0]);
    return poll_rc == 0 ? "child timed out" : "poll failed";
  }

  char buf[256];
  const ssize_t n = read(fds[0], buf, sizeof(buf));
  close(fds[0]);
  int status = 0;
  waitpid(pid, &status, 0);
  if (n <= 0) {
    return "child exited without a verdict";
  }
  return std::string(buf, n);
}

}  // namespace

TEST(QuotaMetricStarvationTest, WriterAloneDoesNotStarveMutations) {
  EXPECT_EQ("OK", RunInChild(true, false, 4));
}

TEST(QuotaMetricStarvationTest, ScrapesAloneDoNotStarveMutations) {
  EXPECT_EQ("OK", RunInChild(false, true, 4));
}

TEST(QuotaMetricStarvationTest, YieldingGetterDoesNotStarveMutations) {
  EXPECT_EQ("OK", RunInChild(true, true, kMutationCount))
      << "brpc#3470 must run bvar callbacks outside the VarMap lock";
}

}  // namespace unit_test
}  // namespace mds
}  // namespace dingofs
