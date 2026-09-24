#!/usr/bin/env python3
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

"""Exercise build identities using real Git repositories and the CMake module."""

import os
from pathlib import Path
import subprocess
import tempfile
import unittest


MODULE = Path(__file__).resolve().parents[2] / "cmake" / "GitVersion.cmake"


class GitVersionTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="dingofs-git-version-")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.repo = self.root / "source"
        self.repo.mkdir()
        self.env = os.environ.copy()
        for name in ("GITHUB_REF_NAME", "GITHUB_SHA", "GIT_DIR", "GIT_WORK_TREE",
                     "GIT_INDEX_FILE"):
            self.env.pop(name, None)
        self.env.update({
            "GIT_CONFIG_GLOBAL": os.devnull,
            "GIT_CONFIG_NOSYSTEM": "1",
            "GIT_AUTHOR_NAME": "chuandew",
            "GIT_AUTHOR_EMAIL": "chuandew@example.com",
            "GIT_COMMITTER_NAME": "chuandew",
            "GIT_COMMITTER_EMAIL": "chuandew@example.com",
        })
        self.git("init", "--quiet", "--initial-branch=Feature/Version")
        (self.repo / "tracked.txt").write_text("original\n")
        self.git("add", "tracked.txt")
        self.git("commit", "--quiet", "-m", "Initial fixture")
        self.git("tag", "v4.2.0")
        self.git("commit", "--quiet", "--allow-empty", "-m", "Advance fixture")
        self.commit = self.git("rev-parse", "HEAD")
        self.short = self.git("rev-parse", "--short", "HEAD")

    def git(self, *args):
        return subprocess.check_output(
            ["git", *args], cwd=self.repo, env=self.env, text=True
        ).strip()

    def configure(self, source=None, cicd_build=False, **environment):
        source = source or self.repo
        result = self.root / "metadata.txt"
        script = self.root / "version.cmake"
        script.write_text(
            f'set(PROJECT_SOURCE_DIR "{source.as_posix()}")\n'
            f'set(USE_CICD_BUILD {"ON" if cicd_build else "OFF"})\n'
            f'include("{MODULE.as_posix()}")\n'
            f'file(WRITE "{result.as_posix()}" '
            '"version=${GIT_VERSION}\\n'
            'source=${DINGOFS_BUILD_SOURCE}\\n")\n'
        )
        # Deliberately invoke from another Git checkout: metadata must come
        # from PROJECT_SOURCE_DIR, not the calling process's working directory.
        subprocess.run(
            ["cmake", "-P", str(script)], cwd=MODULE.parents[1],
            env={**self.env, **environment}, text=True,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True,
        )
        return dict(line.split("=", 1) for line in result.read_text().splitlines())

    def test_attached_branch_preserves_case_and_ignores_ci_ref(self):
        metadata = self.configure(GITHUB_REF_NAME="main", GITHUB_SHA=self.commit)
        self.assertEqual(metadata["version"], f"Feature/Version-{self.short}")

    def test_false_constant_branch_preserves_attached_identity(self):
        self.git("branch", "-m", "OFF")
        self.assertEqual(self.configure()["version"], f"OFF-{self.short}")
        metadata = self.configure(GITHUB_REF_NAME="main", GITHUB_SHA=self.commit)
        self.assertEqual(metadata["version"], f"OFF-{self.short}")

    def test_detached_checkout_without_ci_is_explicit(self):
        self.git("checkout", "--quiet", "--detach")
        self.assertEqual(self.configure()["version"], f"detached-{self.short}")

    def test_detached_ci_tag_identifies_the_checked_out_commit(self):
        self.git("checkout", "--quiet", "--detach")
        metadata = self.configure(GITHUB_REF_NAME="v5.2.0", GITHUB_SHA=self.commit)
        self.assertEqual(metadata["version"], f"v5.2.0-{self.short}")

    def test_detached_checkout_rejects_ci_metadata_for_another_commit(self):
        self.git("checkout", "--quiet", "--detach")
        metadata = self.configure(GITHUB_REF_NAME="main", GITHUB_SHA="0" * 40)
        self.assertEqual(metadata["version"], f"detached-{self.short}")

    def test_source_without_git_does_not_inherit_the_callers_identity(self):
        source = self.root / "archive"
        source.mkdir()
        metadata = self.configure(source, GITHUB_REF_NAME="main", GITHUB_SHA=self.commit)
        self.assertEqual(metadata["version"], "detached-unknown")

    def test_unborn_branch_keeps_its_name_without_a_commit(self):
        source = self.root / "unborn"
        self.git("init", "--quiet", "--initial-branch=Feature/Unborn", str(source))
        metadata = self.configure(source, GITHUB_REF_NAME="main", GITHUB_SHA=self.commit)
        self.assertEqual(metadata["version"], "Feature/Unborn-unknown")

    def test_build_source_is_explicit_and_does_not_change_identity(self):
        local = self.configure(GITHUB_REF_NAME="main", GITHUB_SHA=self.commit)
        ci = self.configure(cicd_build=True)
        self.assertEqual(local["source"], "local")
        self.assertEqual(ci["source"], "ci/cd")
        self.assertEqual(local["version"], ci["version"])

    def test_git_state_errors_are_not_reported_as_clean(self):
        (self.repo / ".git" / "index").write_bytes(b"invalid")
        self.assertEqual(self.configure()["version"],
                         f"Feature/Version-{self.short}-unknown-state")

    def test_dirty_suffix_tracks_changes_without_changing_the_commit(self):
        clean = f"Feature/Version-{self.short}"
        self.assertEqual(self.configure()["version"], clean)
        (self.repo / "tracked.txt").write_text("modified\n")
        self.assertEqual(self.configure()["version"], clean + "-dirty")
        self.git("add", "tracked.txt")
        self.assertEqual(self.configure()["version"], clean + "-dirty")
        self.git("restore", "--source=HEAD", "--staged", "--worktree",
                 "tracked.txt")
        self.assertEqual(self.configure()["version"], clean)
        (self.repo / "untracked.txt").write_text("build artifact\n")
        self.assertEqual(self.configure()["version"], clean)


if __name__ == "__main__":
    unittest.main()
