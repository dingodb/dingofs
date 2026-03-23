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
 * test_binding_logging.cc
 *
 * Group 1 — init_glog ownership (2 cases)
 *   init_glog=true  : BindingClient calls Logger::Init(), shuts down on Stop()
 *   init_glog=false : BindingClient leaves glog untouched
 *
 * Group 2 — log_dir priority (3 cases, all with init_glog=true)
 *   explicit log_dir  : logs written to config.log_dir
 *   conf_file log_dir : logs written to path from conf_file
 *   no log_dir set    : logs written to /tmp (matches glog's own default)
 *
 * --- Subprocess design ---
 * DingoFS registers spdlog loggers once per process at BindingClient
 * construction time.  Creating a second instance in the same process
 * triggers duplicate-logger abort.  Each test case therefore runs as its
 * own child process via fork+exec.
 *
 *   ./test_binding_logging              ← parent: runs all children
 *   ./test_binding_logging <case-id>    ← child: runs one case
 */

#include <cstdio>
#include <cstring>
#include <dirent.h>
#include <string>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#include <glog/logging.h>

#include "binding_client.h"

using dingofs::client::BindingClient;
using dingofs::client::BindingConfig;

/* ============================================================================
 * Minimal test runner
 * ============================================================================ */

static int g_pass = 0;
static int g_fail = 0;

#define PASS(n) \
    do { printf("[ PASS ] %s\n", (n)); ++g_pass; } while (0)

#define ASSERT(n, cond)                                                    \
    do {                                                                   \
        if (!(cond)) {                                                     \
            printf("[ FAIL ] %s — assertion failed: %s\n", (n), #cond);   \
            ++g_fail;                                                      \
            return;                                                        \
        }                                                                  \
    } while (0)

/* ============================================================================
 * Filesystem helpers
 * ============================================================================ */

static std::string make_tmp_dir(const char* suffix) {
    std::string path = std::string("/tmp/dingofs-log-test-") + suffix;
    mkdir(path.c_str(), 0755);
    return path;
}

static void rm_dir(const std::string& dir) {
    DIR* d = opendir(dir.c_str());
    if (!d) return;
    struct dirent* ent;
    while ((ent = readdir(d)) != nullptr) {
        if (ent->d_name[0] == '.') continue;
        unlink((dir + "/" + ent->d_name).c_str());
    }
    closedir(d);
    rmdir(dir.c_str());
}

/* True when at least one entry in dir starts with prefix. */
static bool dir_has_file_with_prefix(const std::string& dir,
                                     const std::string& prefix) {
    DIR* d = opendir(dir.c_str());
    if (!d) return false;
    struct dirent* ent;
    bool found = false;
    while ((ent = readdir(d)) != nullptr) {
        if (strncmp(ent->d_name, prefix.c_str(), prefix.size()) == 0) {
            found = true;
            break;
        }
    }
    closedir(d);
    return found;
}

/* Emit LOG(INFO), flush, then check that a file with the given prefix exists
 * in the given directory.  Returns true on success. */
static bool check_log_file_in(const std::string& dir, const std::string& prefix,
                               const char* test_name) {
    LOG(INFO) << test_name << " marker";
    google::FlushLogFiles(google::GLOG_INFO);
    return dir_has_file_with_prefix(dir, prefix);
}

/* Build a minimal BindingConfig with an unreachable MDS. */
static BindingConfig make_config(const std::string& log_dir,
                                 const std::string& conf_file = "") {
    BindingConfig cfg;
    cfg.mds_addrs   = "127.0.0.1:9999";
    cfg.fs_name     = "test-fs";
    cfg.mount_point = "/mnt/test";
    cfg.init_glog   = true;
    cfg.log_dir     = log_dir;
    cfg.conf_file   = conf_file;
    cfg.log_level   = "INFO";
    return cfg;
}

/* ============================================================================
 * Group 1 — init_glog ownership
 * ============================================================================ */

/* init_glog=true: BindingClient initialises glog and shuts it down on Stop(). */
static void test_init_glog_true() {
    const char* name = "init_glog=true  →  BindingClient owns glog";

    std::string log_dir = make_tmp_dir("own-true");

    ASSERT(name, !google::IsGoogleLoggingInitialized());

    {
        BindingClient client;
        client.Start(make_config(log_dir));

        ASSERT(name, google::IsGoogleLoggingInitialized());
        ASSERT(name, check_log_file_in(log_dir, "dingofs-binding.info.log.", name));

        client.Stop();
        ASSERT(name, !google::IsGoogleLoggingInitialized());
    }

    rm_dir(log_dir);
    PASS(name);
}

/* init_glog=false: BindingClient must not touch glog at all. */
static void test_init_glog_false() {
    const char* name = "init_glog=false  →  BindingClient leaves glog alone";

    std::string log_dir = make_tmp_dir("own-false");

    FLAGS_log_dir = log_dir;
    google::InitGoogleLogging("test-app");
    google::SetStderrLogging(google::GLOG_FATAL);
    ASSERT(name, google::IsGoogleLoggingInitialized());

    {
        BindingConfig cfg;
        cfg.mds_addrs   = "127.0.0.1:9999";
        cfg.fs_name     = "test-fs";
        cfg.mount_point = "/mnt/test";
        cfg.init_glog   = false;

        BindingClient client;
        client.Start(cfg);

        ASSERT(name, !dir_has_file_with_prefix(log_dir, "dingofs-binding."));

        client.Stop();
        ASSERT(name, google::IsGoogleLoggingInitialized());
    }

    google::ShutdownGoogleLogging();
    ASSERT(name, !google::IsGoogleLoggingInitialized());

    rm_dir(log_dir);
    PASS(name);
}

/* ============================================================================
 * Group 2 — log_dir priority
 *
 * All three cases use init_glog=true.  glog creates files as:
 *   {log_dir}/dingofs-binding.info.log.{timestamp}
 * so we check for that prefix in the expected directory.
 * ============================================================================ */

/* Case 1: config.log_dir explicitly set → logs go there. */
static void test_log_dir_explicit() {
    const char* name = "log_dir: explicit config.log_dir  →  logs in that dir";

    std::string log_dir = make_tmp_dir("dir-explicit");

    {
        BindingClient client;
        client.Start(make_config(log_dir));

        ASSERT(name, check_log_file_in(log_dir, "dingofs-binding.info.log.", name));

        client.Stop();
    }

    rm_dir(log_dir);
    PASS(name);
}

/* Case 2: conf_file sets log_dir → logs go to the path from conf_file. */
static void test_log_dir_from_conf_file() {
    const char* name = "log_dir: set via conf_file  →  logs in conf_file dir";

    std::string log_dir  = make_tmp_dir("dir-conffile");
    std::string conf_path = "/tmp/dingofs-log-test-dir-conffile.conf";

    /* Write a gflags conf file with log_dir set. */
    FILE* f = fopen(conf_path.c_str(), "w");
    ASSERT(name, f != nullptr);
    fprintf(f, "--log_dir=%s\n", log_dir.c_str());
    fclose(f);

    {
        BindingConfig cfg = make_config(/*log_dir=*/"", conf_path);
        BindingClient client;
        client.Start(cfg);

        ASSERT(name, check_log_file_in(log_dir, "dingofs-binding.info.log.", name));

        client.Stop();
    }

    unlink(conf_path.c_str());
    rm_dir(log_dir);
    PASS(name);
}

/* Case 3: neither config.log_dir nor conf_file sets log_dir.
 * Expected: logs go to /tmp, matching glog's own default.
 * We must NOT see logs in ~/.dingofs/log/. */
static void test_log_dir_default_tmp() {
    const char* name = "log_dir: not set  →  logs in /tmp (glog default)";

    {
        BindingConfig cfg;
        cfg.mds_addrs   = "127.0.0.1:9999";
        cfg.fs_name     = "test-fs";
        cfg.mount_point = "/mnt/test";
        cfg.init_glog   = true;
        cfg.log_level   = "INFO";
        /* log_dir and conf_file intentionally left empty */

        BindingClient client;
        client.Start(cfg);

        ASSERT(name, check_log_file_in("/tmp", "dingofs-binding.info.log.", name));

        /* DingoFS default dir (~/.dingofs/log/) must NOT have been used. */
        const char* dingofs_default = getenv("HOME");
        if (dingofs_default) {
            std::string default_log = std::string(dingofs_default) + "/.dingofs/log";
            ASSERT(name, !dir_has_file_with_prefix(default_log,
                                                   "dingofs-binding.info.log."));
        }

        client.Stop();
    }

    /* Clean up the /tmp log files we just created. */
    DIR* d = opendir("/tmp");
    if (d) {
        struct dirent* ent;
        while ((ent = readdir(d)) != nullptr) {
            if (strncmp(ent->d_name, "dingofs-binding.", 16) == 0)
                unlink((std::string("/tmp/") + ent->d_name).c_str());
        }
        closedir(d);
    }

    PASS(name);
}

/* ============================================================================
 * Entry point
 * ============================================================================ */

static int run_child(const char* exe, const char* arg) {
    pid_t pid = fork();
    if (pid < 0) { perror("fork"); return -1; }
    if (pid == 0) { execl(exe, exe, arg, nullptr); _exit(127); }
    int status = 0;
    waitpid(pid, &status, 0);
    return WIFEXITED(status) ? WEXITSTATUS(status) : -1;
}

int main(int argc, char* argv[]) {
    /* ── subprocess: run exactly one test case ── */
    if (argc >= 2) {
        const char* id = argv[1];
        if      (strcmp(id, "own-true")     == 0) test_init_glog_true();
        else if (strcmp(id, "own-false")    == 0) test_init_glog_false();
        else if (strcmp(id, "dir-explicit") == 0) test_log_dir_explicit();
        else if (strcmp(id, "dir-conffile") == 0) test_log_dir_from_conf_file();
        else if (strcmp(id, "dir-default")  == 0) test_log_dir_default_tmp();
        else { fprintf(stderr, "unknown case: %s\n", id); return 1; }
        return g_fail > 0 ? 1 : 0;
    }

    /* ── parent: fork one child per case ── */
    printf("=== BindingClient logging tests ===\n\n");

    struct { const char* id; const char* label; } cases[] = {
        /* Group 1: init_glog ownership */
        {"own-true",     "[own] init_glog=true   →  BindingClient owns glog"},
        {"own-false",    "[own] init_glog=false  →  BindingClient leaves glog alone"},
        /* Group 2: log_dir priority */
        {"dir-explicit", "[dir] explicit config.log_dir  →  logs in that dir"},
        {"dir-conffile", "[dir] log_dir via conf_file     →  logs in conf_file dir"},
        {"dir-default",  "[dir] no log_dir set            →  logs in /tmp"},
    };

    int total_pass = 0, total_fail = 0;
    for (const auto& c : cases) {
        int rc = run_child(argv[0], c.id);
        if (rc == 0) {
            printf("[ PASS ] %s\n", c.label);
            ++total_pass;
        } else {
            printf("[ FAIL ] %s  (exit %d)\n", c.label, rc);
            ++total_fail;
        }
    }

    printf("\n=== Results: %d passed, %d failed ===\n", total_pass, total_fail);
    return total_fail > 0 ? 1 : 0;
}
