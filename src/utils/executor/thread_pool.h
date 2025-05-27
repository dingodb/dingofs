// Copyright (c) 2025 dingodb.com, Inc. All Rights Reserved
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

#ifndef DINGOFS_UITLS_THREAD_POOL_H_
#define DINGOFS_UITLS_THREAD_POOL_H_

#include <functional>

namespace dingofs {

class ThreadPool {
 public:
  ThreadPool(int num_threads);

  virtual ~ThreadPool();

  virtual void Start();

  virtual void Stop();

  virtual int GetBackgroundThreads();

  // Get the number of task scheduled in the ThreadPool
  virtual int GetTaskNum() const;

  // Submit a fire and forget jobs
  // This allows to submit the same job multiple times
  virtual void Execute(const std::function<void()>&);

  // This moves the function in for efficiency
  virtual void Execute(std::function<void()>&&);

 private:
  class Impl;
  Impl* impl_;
};

// NewThreadPool() is a function that could be used to create a ThreadPool
// with `num_threads` background threads.
ThreadPool* NewThreadPool(int num_threads);

}  // namespace dingofs

#endif  // DINGOFS_UITLS_THREAD_POOL_H_