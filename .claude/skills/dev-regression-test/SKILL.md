---
name: dev-regression-test
description: 在开发环境进行回归测试，当开发完成功能或修复bug后，可以使用这个技能在开发环境进行回归测试，验证代码变更是否生效，是否引入新问题。
context: fork
disable-model-invocation: true
---


# dingofs回归测试技能
**注意**: 本技能仅适用于开发环境部署测试dingofs，不要用于生产环境部署，并只进行基本功能回归验证，提交代码之前可以使用这个技能在开发环境进行回归测试，验证代码变更是否生效，是否引入新问题。

使用方式: /dev-regression-test [测试目录路径]

## 回归测试工具

### pjdtest工具
主要用于测试元数据操作是否正常，是否符合 POXIS 标准，测试内容包括创建文件、删除文件、重命名文件、创建目录、删除目录、重命名目录等操作，测试结果会输出到日志目录下，可以查看日志分析测试结果。
```bash

# 测试目录
PJD_TEST_DIR=$ARGUMENTS[0]/pjd_test_$(date +%Y%m%d%H%M%S)
# 创建测试目录和日志目录
mkdir -p ${PJD_TEST_DIR}
mkdir -p ${PJD_TEST_DIR}/log

# 运行pjdtest工具，测试结果会输出到${PJD_TEST_DIR}/log目录下
podman run --rm -v ${PJD_TEST_DIR}:/data dingofs-benchmark-tools -t pjdtest -s pjdtest -m /data -o /data/log

```


### mdtest工具
主要用于测试元数据性能，测试内容包括创建文件、删除文件、重命名文件、创建目录、删除目录、重命名目录等操作，测试结果会输出到日志目录下，可以查看日志分析测试结果。
```bash

# 测试目录
MDTEST_TEST_DIR=$ARGUMENTS[0]/mdtest_test_$(date +%Y%m%d%H%M%S)
# 创建测试目录和日志目录
mkdir -p ${MDTEST_TEST_DIR}
mkdir -p ${MDTEST_TEST_DIR}/log

# 运行mdtest工具，测试结果会输出到${MDTEST_TEST_DIR}/log目录下
# scene参数:
#   mdtest_z0_n100: 目录深度为0，每个目录创建100个文件
#   mdtest_z5_b4_i1: 目录深度为5，目录分支大小为4，每个目录创建1个文件
#   mdtest_z6_b3_i1: 目录深度为6，目录分支大小为3，每个目录创建1个文件
#   mdtest_z9_b2_i1: 目录深度为9，目录分支大小为2，每个目录创建1个文件
podman run --rm -v ${MDTEST_TEST_DIR}:/data dingofs-benchmark-tools -t mdtest -s $scene -m /data -o /data/log


```


### vdbench工具
主要用户测试数据读写性能，测试内容包括顺序读写和随机读写，测试结果会输出到日志目录下，可以查看日志分析测试结果。
```bash

# 测试目录
VDBENCH_TEST_DIR=$ARGUMENTS[0]/vdbench_test_$(date +%Y%m%d%H%M%S)
# 创建测试目录和日志目录
mkdir -p ${VDBENCH_TEST_DIR}
mkdir -p ${VDBENCH_TEST_DIR}/log

# 运行pjdtest工具，测试结果会输出到${VDBENCH_TEST_DIR}/log目录下
# scene参数:
#  seq_wr: 顺序写测试
#  seq_rd: 顺序读测试
#  rand_wr: 随机写测试
#  rand_rd: 随机读测试
podman run --rm -v ${VDBENCH_TEST_DIR}:/data dingofs-benchmark-tools -t pjdtest -s $scene -m /data -o /data/log

```


### fio工具
主要用于测试数据读写性能，测试内容包括顺序读写和随机读写，测试结果会输出到日志目录下，可以查看日志分析测试结果。
```bash

# 测试目录
FIO_TEST_DIR=$ARGUMENTS[0]/fio_test_$(date +%Y%m%d%H%M%S)
# 创建测试目录和日志目录
mkdir -p ${FIO_TEST_DIR}
mkdir -p ${FIO_TEST_DIR}/log

# 运行fio工具，测试结果会输出到${FIO_TEST_DIR}/log目录下
# scene参数:
#  rand_read_0d_128k_16j: 随机读测试，目录深度为0，每个目录创建128KB的文件，16个并发作业
#  rand_read_0d_128k_1j: 顺序读测试，目录深度为0，每个目录创建128KB的文件，1个并发作业
#  rand_write_0d_128k_16j: 随机写测试，目录深度为0，每个目录创建128KB的文件，16个并发作业
#  rand_write_0d_1m_16j: 顺序写测试，目录深度为0，每个目录创建1MB的文件，16个并发作业

podman run --rm -v ${FIO_TEST_DIR}:/data dingofs-benchmark-tools -t fio -s $scene -m /data -o /data/log

```
