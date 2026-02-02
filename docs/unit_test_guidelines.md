# DingoFS 单元测试编写指南

## 0. 核心原则

1. **深入理解源码**：编写测试前必须仔细阅读被测代码的实现，理解其行为和边界条件
2. **生成单一二进制**：所有测试编译成一个可执行文件（如 `test_cache`），通过 `--gtest_filter` 过滤运行
3. **参考现有测试**：编译方式参考 `test/unit/mds` 的结构
4. **Author 署名**：文件头的 Author 使用 `Wine93`

## 1. 目录结构

测试文件放置在 `test/unit/<module>/` 目录下，与源码目录结构对应：
```
test/unit/cache/
├── CMakeLists.txt          # 主 CMakeLists，生成单一二进制
├── main.cc                 # gtest 主入口
├── common/                 # 对应 src/cache/common
│   ├── CMakeLists.txt
│   ├── test_*.cc
│   └── mock/
├── iutil/                  # 对应 src/cache/iutil
├── blockcache/             # 对应 src/cache/blockcache
├── remotecache/            # 对应 src/cache/remotecache
├── cachegroup/             # 对应 src/cache/cachegroup
└── tiercache/              # 对应 src/cache/tiercache
```

## 2. CMakeLists 配置

### 子目录 CMakeLists.txt（使用 OBJECT 库）
```cmake
file(GLOB TEST_CACHE_XXX_SRCS "*.cc")

add_library(test_cache_xxx OBJECT
    ${TEST_CACHE_XXX_SRCS}
)

target_link_libraries(test_cache_xxx
    cache_lib
    ${TEST_DEPS_WITHOUT_MAIN}
)
```

### 主目录 CMakeLists.txt（链接所有 OBJECT 库）
```cmake
add_subdirectory(common)
add_subdirectory(iutil)
# ... 其他子目录

add_executable(test_cache
    main.cc
    $<TARGET_OBJECTS:test_cache_common>
    $<TARGET_OBJECTS:test_cache_iutil>
    # ... 其他 OBJECT 库
)

target_link_libraries(test_cache
    cache_lib
    ${TEST_DEPS_WITHOUT_MAIN}
)
```

## 3. 文件头信息（必须）

每个 .cc 和 .h 文件必须包含以下头信息：
```cpp
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
 * Author: Wine93
 */
```

## 4. 测试文件命名

- 测试文件：`test_<被测模块>.cc`
- Mock 文件：`mock/mock_<被测类>.h`
- 测试类名：`<被测类>Test`
- 测试用例名：描述性名称，如 `Construction`、`BasicOperation`、`ErrorHandling`

## 5. 测试编写规范

### 基本结构
```cpp
#include <gtest/gtest.h>
#include "cache/xxx/被测头文件.h"

namespace dingofs {
namespace cache {

class XxxTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // 初始化
  }
  void TearDown() override {
    // 清理
  }
};

TEST_F(XxxTest, TestCaseName) {
  // 测试代码
  EXPECT_EQ(expected, actual);
}

}  // namespace cache
}  // namespace dingofs
```

### 测试要点
1. **深入理解源码**：测试前仔细阅读被测代码的实现
2. **覆盖边界情况**：空值、零值、最大值、错误条件
3. **不依赖默认值**：如果默认值来自 FLAGS，不要假设其为空/零
4. **避免实现依赖**：测试行为而非实现细节（如不假设内存池的重用顺序）
5. **检查构造函数签名**：编写测试前先查看源码中类的构造函数参数

## 6. main.cc 模板

```cpp
#include <gflags/gflags.h>
#include <gtest/gtest.h>

DECLARE_int32(v);

int main(int argc, char* argv[]) {
  testing::InitGoogleTest(&argc, argv);
  FLAGS_v = 10;
  return RUN_ALL_TESTS();
}
```

## 7. 编译和运行

```bash
# 编译
cd build && make test_cache -j8

# 运行所有测试
./bin/test/test_cache

# 过滤运行特定测试
./bin/test/test_cache --gtest_filter="XxxTest.*"
./bin/test/test_cache --gtest_filter="*Construction*"
```

## 8. Git 提交规范

- 提交信息格式：`[test][cache] Add comprehensive unit tests for cache modules`
- 修复提交格式：`[test][cache] Fix xxx issue`
- 合并多个小提交为一个完整提交
- 使用 `git commit --amend` 追加修改到已有提交

## 9. PR 流程

1. 推送到个人 remote：`git push wine93 <branch>`
2. 向 origin 发起 PR
3. **监控 review 意见并及时修改**
4. 修改后 amend 提交并 force push：`git push wine93 <branch> -f`
5. 在 GitHub 页面回复 reviewer："已修改，老爷。"

## 10. 常见问题

### 编译错误：找不到构造函数
检查源码中类的构造函数签名，确保测试中使用正确的参数。

### 测试失败：默认值不匹配
如果类的默认值来自 gflags，不要假设其为空或零，只测试可控的行为。

### Mock 对象使用
使用 gmock 创建 mock 对象，放在 `mock/` 子目录中。

## 11. Review 意见速查

| 常见问题 | 解决方案 |
|---------|---------|
| 缺少文件头信息 | 添加 Copyright + Project/Date/Author 块 |
| Author 格式不对 | 使用 `Author: Wine93` |
| 测试假设默认值 | 不假设 FLAGS 默认值，只测试可控行为 |
| 测试依赖实现细节 | 测试行为而非内部实现 |
| 构造函数参数错误 | 先查看源码中的构造函数签名 |
