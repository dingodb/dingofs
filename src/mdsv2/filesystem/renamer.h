// Copyright (c) 2023 dingodb.com, Inc. All Rights Reserved
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

#ifndef DINGOFS_MDV2_FILESYSTEM_RENAMER_H_
#define DINGOFS_MDV2_FILESYSTEM_RENAMER_H_

#include <memory>

#include "mdsv2/common/runnable.h"
#include "mdsv2/common/status.h"

namespace dingofs {
namespace mdsv2 {

class FileSystem;
using FileSystemPtr = std::shared_ptr<FileSystem>;

using RenameCbFunc = std::function<void(Status)>;

class RenameTask : public TaskRunnable {
 public:
  RenameTask(FileSystemPtr fs, uint64_t old_parent_ino, const std::string& old_name, uint64_t new_parent_ino,
             const std::string& new_name, RenameCbFunc cb)
      : fs_(fs),
        old_parent_ino_(old_parent_ino),
        old_name_(old_name),
        new_parent_ino_(new_parent_ino),
        new_name_(new_name),
        cb_(cb) {}

  ~RenameTask() override = default;

  std::string Type() override { return "RENAME"; }

  void Run() override;

 private:
  uint64_t old_parent_ino_;
  std::string old_name_;
  uint64_t new_parent_ino_;
  std::string new_name_;

  RenameCbFunc cb_;
  FileSystemPtr fs_;
};

class Renamer {
 public:
  Renamer() = default;
  ~Renamer() = default;

  Renamer(const Renamer&) = delete;
  Renamer& operator=(const Renamer&) = delete;

  bool Init();
  bool Destroy();

  bool Execute(FileSystemPtr fs, uint64_t old_parent_ino, const std::string& old_name, uint64_t new_parent_ino,
               const std::string& new_name, RenameCbFunc cb);

 private:
  bool Execute(TaskRunnablePtr task);

  WorkerPtr worker_;
};
using RenamerPtr = std::shared_ptr<Renamer>;

}  // namespace mdsv2
}  // namespace dingofs

#endif  // DINGOFS_MDV2_FILESYSTEM_RENAMER_H_
