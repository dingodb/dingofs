// Copyright (c) 2023 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "mds/common/crontab.h"

#include <glog/logging.h>

#include "bthread/bthread.h"
#include "bthread/unstable.h"
#include "common/logging.h"
#include "fmt/core.h"

namespace dingofs {
namespace mds {

void Crontab::DescribeByJson(Json::Value& value) const {
  value["id"] = id;
  value["name"] = name;
  value["interval_ms"] = interval;
  value["max_times"] = max_times;
  value["immediately"] = immediately;
  value["run_count"] = run_count;
  value["pause"] = pause.load();
}

CrontabManager::CrontabManager() { bthread_mutex_init(&mutex_, nullptr); }

CrontabManager::~CrontabManager() {
  // Crontab::Run() reschedules itself via bthread_timer_add() using a raw
  // Crontab* (see below). If crontabs_ (and the Crontabs it keeps alive)
  // were destroyed without first cancelling those pending timers, the
  // timer thread could still invoke Run() on a freed Crontab later,
  // causing a use-after-free. Destroy() cancels every pending timer and
  // waits for in-flight callbacks to finish before we let crontabs_ go.
  Destroy();
  bthread_mutex_destroy(&mutex_);
}

void CrontabManager::Run(void* arg) {
  Crontab* crontab = static_cast<Crontab*>(arg);
  if (crontab->pause) {
    return;
  }
  if (crontab->immediately) {
    try {
      crontab->func(crontab->arg);
    } catch (...) {
      LOG(ERROR) << fmt::format("[crontab.run][id({}).name({})] crontab happen exception", crontab->id, crontab->name);
    }
    ++crontab->run_count;
  } else {
    crontab->immediately = true;
  }

  // Re-check pause: Destroy()/PauseCrontab() may have paused this crontab
  // while func() above was executing. Without this check we could keep
  // rearming a timer after the owning CrontabManager decided to stop (and
  // is waiting to release the Crontab), leading to a use-after-free once
  // the manager is destroyed.
  if (!crontab->pause && (crontab->max_times == 0 || crontab->run_count < crontab->max_times)) {
    bthread_timer_add(&crontab->timer_id, butil::milliseconds_from_now(crontab->interval), &Run, crontab);
  }
}

uint32_t CrontabManager::AllocCrontabId() { return auinc_crontab_id_.fetch_add(1); }

void CrontabManager::AddCrontab(std::vector<CrontabConfig>& crontab_configs) {
  for (auto& crontab_config : crontab_configs) {
    LOG(INFO) << fmt::format("[crontab.add][name({}).interval({}ms).async({})] add crontab task.", crontab_config.name,
                             crontab_config.interval, crontab_config.async);

    auto crontab = std::make_shared<Crontab>();
    crontab->name = crontab_config.name;
    crontab->interval = crontab_config.interval;
    if (crontab_config.async) {
      crontab->func = [&](void*) {
        bthread_t tid;
        const bthread_attr_t attr = BTHREAD_ATTR_NORMAL;
        bthread_start_background(
            &tid, &attr,
            [](void* arg) -> void* {
              CrontabConfig* crontab_config = static_cast<CrontabConfig*>(arg);
              crontab_config->funcer(nullptr);
              return nullptr;
            },
            &crontab_config);
      };
    } else {
      crontab->func = crontab_config.funcer;
    }

    crontab->arg = nullptr;

    this->AddAndRunCrontab(crontab);
  }
}

uint32_t CrontabManager::AddAndRunCrontab(CrontabSPtr crontab) {
  uint32_t crontab_id = AddCrontab(crontab);
  StartCrontab(crontab_id);

  return crontab_id;
}

uint32_t CrontabManager::AddCrontab(CrontabSPtr crontab) {
  BAIDU_SCOPED_LOCK(mutex_);

  uint32_t crontab_id = AllocCrontabId();
  crontab->id = crontab_id;

  crontabs_[crontab_id] = crontab;
  return crontab_id;
}

void CrontabManager::StartCrontab(uint32_t crontab_id) {
  BAIDU_SCOPED_LOCK(mutex_);

  auto it = crontabs_.find(crontab_id);
  if (it == crontabs_.end()) {
    LOG(WARNING) << fmt::format("[crontab.start][id({})] not exist crontab.", crontab_id);
    return;
  }
  auto crontab = it->second;
  crontab->pause = false;

  bthread_t tid;
  const bthread_attr_t attr = BTHREAD_ATTR_NORMAL;
  bthread_start_background(
      &tid, &attr,
      [](void* arg) -> void* {
        CrontabManager::Run(arg);
        return nullptr;
      },
      crontab.get());
}

void CrontabManager::InnerPauseCrontab(uint32_t crontab_id) {
  auto it = crontabs_.find(crontab_id);
  if (it == crontabs_.end()) {
    LOG(WARNING) << fmt::format("[crontab.pause][id({})] not exist crontab.", crontab_id);
    return;
  }
  auto crontab = it->second;

  crontab->pause = true;
  if (crontab->timer_id != 0) {
    bthread_timer_del(crontab->timer_id);
  }
}

void CrontabManager::PauseCrontab(uint32_t crontab_id) {
  BAIDU_SCOPED_LOCK(mutex_);

  InnerPauseCrontab(crontab_id);
}

void CrontabManager::DeleteCrontab(uint32_t crontab_id) {
  BAIDU_SCOPED_LOCK(mutex_);
  InnerPauseCrontab(crontab_id);

  crontabs_.erase(crontab_id);
}

void CrontabManager::Destroy() {
  BAIDU_SCOPED_LOCK(mutex_);

  // Pause every crontab first so any Run() invocation still in flight sees
  // pause==true and skips its reschedule (see the check added in Run()).
  // Only after that is it safe to cancel timers and drop crontabs_'s
  // shared_ptrs: otherwise a concurrent Run() could rearm a timer that
  // outlives the Crontab it points to.
  for (auto& [_, crontab] : crontabs_) {
    crontab->pause = true;
  }

  for (auto it = crontabs_.begin(); it != crontabs_.end();) {
    while (bthread_timer_del(it->second->timer_id) == 1) {
      bthread_usleep(1000L);  // Wait for timer to be deleted
    }

    it = crontabs_.erase(it);
  }
}

void CrontabManager::DescribeByJson(Json::Value& value) {
  CHECK(value.isArray()) << "value is not array.";

  BAIDU_SCOPED_LOCK(mutex_);

  for (auto& [_, crontab] : crontabs_) {
    Json::Value crontab_value;
    crontab->DescribeByJson(crontab_value);
    value.append(crontab_value);
  }
}

}  // namespace mds
}  // namespace dingofs
