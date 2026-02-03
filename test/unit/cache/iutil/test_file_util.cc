/*
 * Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
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

/*
 * Project: DingoFS
 * Created Date: 2026-02-02
 * Author: AI
 */

#include <fcntl.h>
#include <gtest/gtest.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <map>
#include <set>
#include <string>

#include "cache/iutil/file_util.h"

namespace dingofs {
namespace cache {
namespace iutil {

class FileUtilTest : public ::testing::Test {
 protected:
  void SetUp() override {
    test_dir_ =
        "/tmp/dingofs_test_cache_iutil_file_util_" + std::to_string(getpid());
    std::filesystem::create_directories(test_dir_);
  }

  void TearDown() override { std::filesystem::remove_all(test_dir_); }

  std::string test_dir_;
};

TEST_F(FileUtilTest, PosixError) {
  EXPECT_TRUE(PosixError(0).ok());
  EXPECT_TRUE(PosixError(EINVAL).IsInvalidParam());
  EXPECT_TRUE(PosixError(ENOENT).IsNotFound());
  EXPECT_TRUE(PosixError(EEXIST).IsExist());
  EXPECT_TRUE(PosixError(EIO).IsIoError());
  EXPECT_TRUE(PosixError(ENOSPC).IsIoError());
}

TEST_F(FileUtilTest, StrMode) {
  EXPECT_EQ(StrMode(S_IFREG | 0755), "-rwxr-xr-x");
  EXPECT_EQ(StrMode(S_IFREG | 0644), "-rw-r--r--");
  EXPECT_EQ(StrMode(S_IFDIR | 0755), "drwxr-xr-x");
  EXPECT_EQ(StrMode(S_IFLNK | 0777), "lrwxrwxrwx");
  EXPECT_EQ(StrMode(S_IFSOCK | 0755), "srwxr-xr-x");
  EXPECT_EQ(StrMode(S_IFBLK | 0660), "brw-rw----");
  EXPECT_EQ(StrMode(S_IFCHR | 0666), "crw-rw-rw-");
  EXPECT_EQ(StrMode(S_IFIFO | 0644), "frw-r--r--");

  EXPECT_EQ(StrMode(S_IFREG | S_ISUID | 0755), "-rwsr-xr-x");
  EXPECT_EQ(StrMode(S_IFREG | S_ISGID | 0755), "-rwxr-sr-x");
  EXPECT_EQ(StrMode(S_IFDIR | S_ISVTX | 0755), "drwxr-xr-t");
  EXPECT_EQ(StrMode(S_IFREG | S_ISUID | 0655), "-rwSr-xr-x");
}

TEST_F(FileUtilTest, ParentDir) {
  EXPECT_EQ(ParentDir("/a/b/c"), "/a/b");
  EXPECT_EQ(ParentDir("/a/b"), "/a");
  EXPECT_EQ(ParentDir("/a"), "/");
  EXPECT_EQ(ParentDir("/"), "/");
  EXPECT_EQ(ParentDir("a"), "/");
}

TEST_F(FileUtilTest, FileIsExist) {
  std::string filepath = test_dir_ + "/test_exist.txt";
  EXPECT_FALSE(FileIsExist(filepath));

  std::ofstream ofs(filepath);
  ofs << "test";
  ofs.close();

  EXPECT_TRUE(FileIsExist(filepath));
}

TEST_F(FileUtilTest, MkDirs) {
  {
    std::string path = test_dir_ + "/a/b/c";
    EXPECT_FALSE(std::filesystem::exists(path));
    EXPECT_TRUE(MkDirs(path).ok());
    EXPECT_TRUE(std::filesystem::is_directory(path));

    auto check_dir_permission = [](const std::string& dir) {
      struct stat st;
      EXPECT_EQ(stat(dir.c_str(), &st), 0);
      EXPECT_EQ(st.st_mode & 0777, 0755);
    };
    check_dir_permission(test_dir_ + "/a");
    check_dir_permission(test_dir_ + "/a/b");
    check_dir_permission(test_dir_ + "/a/b/c");
  }

  {
    std::string filepath = test_dir_ + "/file.txt";
    std::ofstream ofs(filepath);
    ofs.close();
    EXPECT_TRUE(MkDirs(filepath).IsNotDirectory());
  }
}

TEST_F(FileUtilTest, Walk) {
  std::string subdir = test_dir_ + "/walk_test";
  std::filesystem::create_directories(subdir);

  std::vector<std::string> files = {"file1.txt", "file2.txt", "file3.txt"};
  for (const auto& file : files) {
    std::ofstream ofs(subdir + "/" + file);
    ofs << "content";
    ofs.close();
  }

  std::vector<std::string> visited_files;
  auto status = Walk(
      subdir, [&](const std::string& prefix, const FileInfo& info) -> Status {
        EXPECT_EQ(prefix, subdir);
        visited_files.push_back(info.name);
        return Status::OK();
      });

  EXPECT_TRUE(status.ok());
  EXPECT_EQ(visited_files.size(), 3);

  std::sort(visited_files.begin(), visited_files.end());
  std::sort(files.begin(), files.end());
  EXPECT_EQ(visited_files, files);
}

TEST_F(FileUtilTest, WalkWithFileInfo) {
  std::string subdir = test_dir_ + "/walk_fileinfo";
  std::filesystem::create_directories(subdir);

  std::string src_file = subdir + "/source.txt";
  std::string hard_link = subdir + "/hardlink.txt";

  {
    std::ofstream ofs(src_file);
    ofs << "test content";
    ofs.close();
  }

  EXPECT_EQ(link(src_file.c_str(), hard_link.c_str()), 0);

  std::map<std::string, FileInfo> visited_files;
  auto status =
      Walk(subdir,
           [&](const std::string& /*prefix*/, const FileInfo& info) -> Status {
             visited_files[info.name] = info;
             return Status::OK();
           });

  EXPECT_TRUE(status.ok());
  EXPECT_EQ(visited_files.size(), 2);

  EXPECT_EQ(visited_files["source.txt"].nlink, 2);
  EXPECT_EQ(visited_files["hardlink.txt"].nlink, 2);
  EXPECT_GT(visited_files["source.txt"].atime.sec, 0);
  EXPECT_GT(visited_files["hardlink.txt"].atime.sec, 0);
  EXPECT_EQ(visited_files["source.txt"].size, 12);
  EXPECT_EQ(visited_files["hardlink.txt"].size, 12);
}

TEST_F(FileUtilTest, WalkLargeDirectory) {
  std::string subdir = test_dir_ + "/walk_large";
  std::filesystem::create_directories(subdir);

  const int file_count = 10000;
  for (int i = 0; i < file_count; i++) {
    std::string filepath = subdir + "/file_" + std::to_string(i) + ".txt";
    std::ofstream ofs(filepath);
    ofs << "content";
    ofs.close();
  }

  int visited_count = 0;
  auto status =
      Walk(subdir,
           [&](const std::string& /*prefix*/, const FileInfo& info) -> Status {
             visited_count++;
             return Status::OK();
           });

  EXPECT_TRUE(status.ok());
  EXPECT_EQ(visited_count, file_count);
}

TEST_F(FileUtilTest, WalkWithSubdirectory) {
  std::string subdir = test_dir_ + "/walk_subdir";
  std::filesystem::create_directories(subdir + "/nested");

  std::ofstream ofs1(subdir + "/file1.txt");
  ofs1 << "content";
  ofs1.close();

  std::ofstream ofs2(subdir + "/nested/file2.txt");
  ofs2 << "content";
  ofs2.close();

  std::vector<std::string> visited_files;
  auto status = Walk(
      subdir, [&](const std::string& prefix, const FileInfo& info) -> Status {
        visited_files.push_back(prefix + "/" + info.name);
        return Status::OK();
      });

  EXPECT_TRUE(status.ok());
  EXPECT_EQ(visited_files.size(), 2);

  std::sort(visited_files.begin(), visited_files.end());
  EXPECT_EQ(visited_files[0], subdir + "/file1.txt");
  EXPECT_EQ(visited_files[1], subdir + "/nested/file2.txt");
}

TEST_F(FileUtilTest, WalkDeepNestedDirectory) {
  std::string subdir = test_dir_ + "/walk_deep";

  const int depth = 10;
  const int subdirs_per_level = 3;
  const int files_per_dir = 2;

  std::function<void(const std::string&, int)> create_structure =
      [&](const std::string& path, int level) {
        std::filesystem::create_directories(path);

        for (int i = 0; i < files_per_dir; i++) {
          std::string filepath = path + "/file_" + std::to_string(i) + ".txt";
          std::ofstream ofs(filepath);
          ofs << "content";
          ofs.close();
        }

        if (level < depth) {
          for (int i = 0; i < subdirs_per_level; i++) {
            std::string subpath = path + "/subdir_" + std::to_string(i);
            create_structure(subpath, level + 1);
          }
        }
      };

  create_structure(subdir, 1);

  int expected_files = 0;
  int dirs = 1;
  for (int l = 1; l <= depth; l++) {
    expected_files += dirs * files_per_dir;
    dirs *= subdirs_per_level;
  }

  std::vector<std::string> visited_files;
  auto status = Walk(
      subdir, [&](const std::string& prefix, const FileInfo& info) -> Status {
        visited_files.push_back(prefix + "/" + info.name);
        return Status::OK();
      });

  EXPECT_TRUE(status.ok());
  EXPECT_EQ(static_cast<int>(visited_files.size()), expected_files);

  std::set<std::string> unique_files(visited_files.begin(),
                                     visited_files.end());
  EXPECT_EQ(unique_files.size(), visited_files.size());
}

TEST_F(FileUtilTest, WalkNotExist) {
  std::string path = test_dir_ + "/not_exist_dir";
  auto status = Walk(path, [](const std::string&, const FileInfo&) -> Status {
    return Status::OK();
  });
  EXPECT_TRUE(status.IsNotFound());
}

TEST_F(FileUtilTest, WalkStopOnError) {
  std::string subdir = test_dir_ + "/walk_stop";
  std::filesystem::create_directories(subdir);

  for (int i = 0; i < 5; i++) {
    std::ofstream ofs(subdir + "/file" + std::to_string(i) + ".txt");
    ofs << "content";
    ofs.close();
  }

  int count = 0;
  std::vector<std::string> visited_files;
  auto status =
      Walk(subdir, [&](const std::string&, const FileInfo& info) -> Status {
        count++;
        visited_files.push_back(info.name);
        if (count >= 2) {
          return Status::Internal("stop");
        }
        return Status::OK();
      });

  EXPECT_TRUE(status.IsInternal());
  EXPECT_EQ(count, 2);
  EXPECT_EQ(visited_files.size(), 2);
  for (const auto& name : visited_files) {
    EXPECT_TRUE(name.find("file") != std::string::npos);
    EXPECT_TRUE(name.find(".txt") != std::string::npos);
  }
}

TEST_F(FileUtilTest, Link) {
  std::string src = test_dir_ + "/link_src.txt";
  std::string dest = test_dir_ + "/link/dest.txt";

  std::ofstream ofs(src);
  ofs << "test";
  ofs.close();

  EXPECT_TRUE(Link(src, dest).ok());
  EXPECT_TRUE(FileIsExist(dest));

  FileInfo info;
  EXPECT_TRUE(Stat(src, &info).ok());
  EXPECT_EQ(info.nlink, 2);
}

TEST_F(FileUtilTest, Unlink) {
  {
    std::string filepath = test_dir_ + "/unlink_test.txt";

    std::ofstream ofs(filepath);
    ofs << "test";
    ofs.close();

    EXPECT_TRUE(FileIsExist(filepath));
    EXPECT_TRUE(Unlink(filepath).ok());
    EXPECT_FALSE(FileIsExist(filepath));
  }

  {
    std::string path = test_dir_ + "/not_exist.txt";
    EXPECT_TRUE(Unlink(path).IsNotFound());
  }
}

TEST_F(FileUtilTest, Rename) {
  {
    std::string oldpath = test_dir_ + "/old.txt";
    std::string newpath = test_dir_ + "/new.txt";

    std::ofstream ofs(oldpath);
    ofs << "test";
    ofs.close();

    EXPECT_TRUE(Rename(oldpath, newpath).ok());
    EXPECT_FALSE(FileIsExist(oldpath));
    EXPECT_TRUE(FileIsExist(newpath));
  }

  {
    std::string oldpath = test_dir_ + "/not_exist.txt";
    std::string newpath = test_dir_ + "/new2.txt";
    EXPECT_TRUE(Rename(oldpath, newpath).IsNotFound());
  }
}

TEST_F(FileUtilTest, Stat) {
  {
    std::string filepath = test_dir_ + "/stat_test.txt";
    std::string content = "Hello, World!";

    int fd = open(filepath.c_str(), O_CREAT | O_WRONLY, 0644);
    EXPECT_GE(fd, 0);
    write(fd, content.c_str(), content.size());
    close(fd);

    fd = open(filepath.c_str(), O_RDONLY);
    EXPECT_GE(fd, 0);
    char buf[64];
    read(fd, buf, sizeof(buf));
    close(fd);

    FileInfo info;
    EXPECT_TRUE(Stat(filepath, &info).ok());
    EXPECT_EQ(info.name, filepath);
    EXPECT_EQ(info.nlink, 1);
    EXPECT_EQ(info.size, static_cast<off_t>(content.size()));
    EXPECT_GT(info.atime.sec, 0);
  }

  {
    std::string filepath = test_dir_ + "/not_exist.txt";
    FileInfo info;
    EXPECT_TRUE(Stat(filepath, &info).IsNotFound());
  }
}

TEST_F(FileUtilTest, StatFS) {
  {
    struct StatFS stat;
    EXPECT_TRUE(StatFS(test_dir_, &stat).ok());
    EXPECT_GT(stat.total_bytes, 0);
    EXPECT_GT(stat.total_files, 0);
    EXPECT_GE(stat.free_bytes, 0);
    EXPECT_GE(stat.free_files, 0);
    EXPECT_GE(stat.free_bytes_ratio, 0.0);
    EXPECT_LE(stat.free_bytes_ratio, 1.0);
    EXPECT_GE(stat.free_files_ratio, 0.0);
    EXPECT_LE(stat.free_files_ratio, 1.0);
  }

  {
    struct StatFS stat;
    EXPECT_TRUE(StatFS("/not/exist/path", &stat).IsNotFound());
  }
}

TEST_F(FileUtilTest, CreateFile) {
  std::string filepath = test_dir_ + "/created.txt";
  int fd = -1;

  EXPECT_FALSE(FileIsExist(filepath));
  EXPECT_TRUE(CreateFile(filepath, 0644, &fd).ok());
  EXPECT_GE(fd, 0);
  EXPECT_TRUE(FileIsExist(filepath));

  struct stat st;
  EXPECT_EQ(stat(filepath.c_str(), &st), 0);
  EXPECT_EQ(st.st_mode & 0777, 0644);

  EXPECT_TRUE(Close(fd).ok());
}

TEST_F(FileUtilTest, OpenFile) {
  std::string filepath = test_dir_ + "/open_test.txt";
  std::ofstream ofs(filepath);
  ofs << "test";
  ofs.close();

  {
    int fd = -1;
    EXPECT_TRUE(OpenFile(filepath, O_RDONLY, &fd).ok());
    EXPECT_GE(fd, 0);

    int flags = fcntl(fd, F_GETFL);
    EXPECT_NE(flags, -1);
    EXPECT_EQ(flags & O_ACCMODE, O_RDONLY);

    EXPECT_TRUE(Close(fd).ok());
  }

  {
    int fd = -1;
    EXPECT_TRUE(OpenFile(filepath, O_RDWR, 0644, &fd).ok());
    EXPECT_GE(fd, 0);

    int flags = fcntl(fd, F_GETFL);
    EXPECT_NE(flags, -1);
    EXPECT_EQ(flags & O_ACCMODE, O_RDWR);

    EXPECT_TRUE(Close(fd).ok());
  }

  {
    std::string new_filepath = test_dir_ + "/open_create.txt";
    int fd = -1;
    EXPECT_FALSE(FileIsExist(new_filepath));
    EXPECT_TRUE(OpenFile(new_filepath, O_CREAT | O_RDWR, 0755, &fd).ok());
    EXPECT_GE(fd, 0);
    EXPECT_TRUE(FileIsExist(new_filepath));

    int flags = fcntl(fd, F_GETFL);
    EXPECT_NE(flags, -1);
    EXPECT_EQ(flags & O_ACCMODE, O_RDWR);

    struct stat st;
    EXPECT_EQ(stat(new_filepath.c_str(), &st), 0);
    EXPECT_EQ(st.st_mode & 0777, 0755);

    EXPECT_TRUE(Close(fd).ok());
  }

  {
    int fd = -1;
    std::string not_exist = test_dir_ + "/not_exist.txt";
    EXPECT_TRUE(OpenFile(not_exist, O_RDONLY, &fd).IsNotFound());
  }
}

TEST_F(FileUtilTest, WriteFile) {
  std::string filepath = test_dir_ + "/nested/dir/test.txt";
  std::string content = "Hello, DingoFS!";

  EXPECT_TRUE(WriteFile(filepath, content).ok());
  EXPECT_TRUE(FileIsExist(filepath));

  std::string read_content;
  std::ifstream ifs(filepath);
  std::getline(ifs, read_content);
  ifs.close();
  EXPECT_EQ(read_content, content);
}

TEST_F(FileUtilTest, ReadFile) {
  {
    std::string filepath = test_dir_ + "/read_test.txt";
    std::string content = "Hello, DingoFS!";

    std::ofstream ofs(filepath);
    ofs << content;
    ofs.close();

    std::string read_content;
    EXPECT_TRUE(ReadFile(filepath, &read_content).ok());
    EXPECT_EQ(read_content, content);
  }

  {
    std::string filepath = test_dir_ + "/not_exist.txt";
    std::string content;
    EXPECT_TRUE(ReadFile(filepath, &content).IsNotFound());
  }
}

TEST_F(FileUtilTest, Fallocate) {
  std::string filepath = test_dir_ + "/fallocate_test.txt";
  int fd = -1;

  EXPECT_TRUE(CreateFile(filepath, 0644, &fd).ok());

  FileInfo info_before;
  EXPECT_TRUE(Stat(filepath, &info_before).ok());
  EXPECT_EQ(info_before.size, 0);

  EXPECT_TRUE(Fallocate(fd, 0, 0, 4096).ok());

  FileInfo info_after;
  EXPECT_TRUE(Stat(filepath, &info_after).ok());
  EXPECT_EQ(info_after.size, 4096);

  EXPECT_TRUE(Close(fd).ok());
}

}  // namespace iutil
}  // namespace cache
}  // namespace dingofs
