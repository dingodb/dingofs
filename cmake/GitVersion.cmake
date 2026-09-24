# Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

execute_process(
  COMMAND git log --pretty=format:%an -1
  WORKING_DIRECTORY "${PROJECT_SOURCE_DIR}"
  OUTPUT_VARIABLE GIT_COMMIT_USER
  OUTPUT_STRIP_TRAILING_WHITESPACE
  ERROR_QUIET)

execute_process(
  COMMAND git log --pretty=format:%ae -1
  WORKING_DIRECTORY "${PROJECT_SOURCE_DIR}"
  OUTPUT_VARIABLE GIT_COMMIT_MAIL
  OUTPUT_STRIP_TRAILING_WHITESPACE
  ERROR_QUIET)

execute_process(
  COMMAND git log --pretty=format:%ai -1
  WORKING_DIRECTORY "${PROJECT_SOURCE_DIR}"
  OUTPUT_VARIABLE GIT_COMMIT_TIME
  OUTPUT_STRIP_TRAILING_WHITESPACE
  ERROR_QUIET)

execute_process(
  COMMAND git rev-parse --verify HEAD
  WORKING_DIRECTORY "${PROJECT_SOURCE_DIR}"
  OUTPUT_VARIABLE GIT_COMMIT_ID
  OUTPUT_STRIP_TRAILING_WHITESPACE
  ERROR_QUIET)

execute_process(
  COMMAND git rev-parse --short HEAD
  WORKING_DIRECTORY "${PROJECT_SOURCE_DIR}"
  OUTPUT_VARIABLE GIT_LAST_COMMIT_ID
  OUTPUT_STRIP_TRAILING_WHITESPACE
  ERROR_QUIET)

execute_process(
  COMMAND git symbolic-ref --quiet --short HEAD
  WORKING_DIRECTORY "${PROJECT_SOURCE_DIR}"
  OUTPUT_VARIABLE GIT_BRANCH_NAME
  OUTPUT_STRIP_TRAILING_WHITESPACE
  ERROR_QUIET)

# A detached CI checkout may use a branch, tag, or merge-queue ref. Only
# accept its name when the CI commit is the commit actually being built.
if(GIT_BRANCH_NAME STREQUAL "")
  if(GIT_COMMIT_ID AND NOT "$ENV{GITHUB_REF_NAME}" STREQUAL ""
      AND "$ENV{GITHUB_SHA}" STREQUAL "${GIT_COMMIT_ID}")
    set(GIT_BRANCH_NAME "$ENV{GITHUB_REF_NAME}")
  else()
    set(GIT_BRANCH_NAME "detached")
  endif()
endif()

if(NOT GIT_LAST_COMMIT_ID)
  set(GIT_LAST_COMMIT_ID "unknown")
  message(WARNING "Git commit id is unknown")
endif()

# Use the actual ref and commit, not the nearest historical release tag.
set(GIT_VERSION "${GIT_BRANCH_NAME}-${GIT_LAST_COMMIT_ID}")

# Match the previous dirty semantics: tracked staged/unstaged changes count,
# but untracked build artifacts do not. A failed check must not imply clean.
if(GIT_COMMIT_ID)
  execute_process(
    COMMAND git diff --quiet HEAD --
    WORKING_DIRECTORY "${PROJECT_SOURCE_DIR}"
    RESULT_VARIABLE GIT_WORKTREE_STATUS
    ERROR_QUIET)
  if("${GIT_WORKTREE_STATUS}" STREQUAL "1")
    string(APPEND GIT_VERSION "-dirty")
  elseif(NOT "${GIT_WORKTREE_STATUS}" STREQUAL "0")
    string(APPEND GIT_VERSION "-unknown-state")
  endif()
endif()

# Build provenance is independent of the checked-out Git ref.
if(USE_CICD_BUILD)
  set(DINGOFS_BUILD_SOURCE "ci/cd")
else()
  set(DINGOFS_BUILD_SOURCE "local")
endif()

# proto/ is the dingofs-proto git submodule; keep its identity for the dashboard.
execute_process(
  COMMAND git -C "${PROJECT_SOURCE_DIR}/proto" log -1 --format=%h
  OUTPUT_VARIABLE PROTO_GIT_COMMIT_ID
  OUTPUT_STRIP_TRAILING_WHITESPACE
  ERROR_QUIET)

if(NOT PROTO_GIT_COMMIT_ID)
  set(PROTO_GIT_COMMIT_ID "unknown")
  message(WARNING "Proto git commit id is unknown")
endif()

message(STATUS "Build version: ${GIT_VERSION}")
message(STATUS "Build source: ${DINGOFS_BUILD_SOURCE}")
message(STATUS "Git commit user: ${GIT_COMMIT_USER}")
message(STATUS "Git commit mail: ${GIT_COMMIT_MAIL}")
message(STATUS "Git commit time: ${GIT_COMMIT_TIME}")
message(STATUS "Git last commit id: ${GIT_LAST_COMMIT_ID}")
message(STATUS "Git branch name: ${GIT_BRANCH_NAME}")
message(STATUS "Proto git commit id: ${PROTO_GIT_COMMIT_ID}")
