# Copyright 2026 DingoDB, Inc.
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

"""Regression test for readdir pagination skipping prefixed names.

Bug:
  When a directory listing spans several batches, the MDS resumed each batch
  from PrefixNext(last_name). For last_name "c33x" that is "c33y", so every
  entry prefixed by "c33x" (e.g. "c33xx") was silently dropped from the
  listing. Names that are prefixes of later names are common.

Fix: commit 17953b56f — resume scan strictly after the last key via
  Helper::KeyNext in ShardPartition::Scan and the TiKV Scan paths.

Verification:
  pre fix  (pre 17953b56f): os.listdir returns fewer names than created
                            (entries lost at the first batch boundary)
  post fix (post 17953b56f): all names are listed
  works on: MDS (ShardPartition::Scan / TiKV scan). Local and Memory have
            their own readdir code and are not affected by this fix.

The client batch size is 1024 (vfs_meta_read_dir_batch_size). The names are
built as 64 chains of 31 names each ("c33x", "c33xx", ...), so the batch
boundary at entry 1024 lands mid-chain.
"""

import os

import pytest

pytestmark = pytest.mark.standard

CHAINS = 64
CHAIN_LEN = 31  # 1024 % 31 != 0, so the batch boundary lands mid-chain


def _chain_names():
    return {f"c{c:02d}" + "x" * k for c in range(CHAINS)
            for k in range(1, CHAIN_LEN + 1)}


def test_readdir_resumes_after_prefix_names(test_dir):
    """Every name must be listed even when a batch boundary falls on a
    name that is a prefix of the following names."""
    names = _chain_names()
    for name in names:
        open(os.path.join(test_dir, name), "wb").close()

    listed = os.listdir(test_dir)
    assert len(listed) == len(names), (
        f"lost {len(names) - len(listed)} entries across batch boundary")
    assert set(listed) == names
