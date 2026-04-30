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

### 基础命令
基础的文件系统操作命令，主要用于测试基本的文件系统功能是否正常，包括创建文件、删除文件、重命名文件、创建目录、删除目录、重命名目录等操作，可以自由使用下面的工具进行测试，根据命令执行结果可以判断基本的文件系统功能是否正常。
```bash
# 下面的工具可以使用--help参数查看具体用法和参数说明

# 创建目录
mkdir

# 删除目录
rm

# 创建文件
touch

# 删除文件
rm

# 重命名文件
mv

# 列出目录内容
ls

# 写入数据到文件
dd

# 显示文件内容
cat

# 显示文件属性
stat

# 修改文件权限
chmod

# 修改文件所有者
chown

# 截断文件
truncate

```

### pjdtest工具
主要用于测试元数据操作是否正常，是否符合 POXIS 标准，测试内容包括创建文件、删除文件、重命名文件、创建目录、删除目录、重命名目录等操作，测试结果会输出到日志目录下，可以查看日志分析测试结果。
```bash

# 测试目录
PJD_TEST_DIR=$ARGUMENTS[0]/pjd_test_$(date +%Y%m%d%H%M%S)
PJD_OUTPUT_DIR=/tmp/dtt
# 创建测试目录和日志目录
mkdir -p ${PJD_TEST_DIR}
mkdir -p ${PJD_OUTPUT_DIR}

# 配置测试目录和日志目录
dtt config set testdir ${PJD_TEST_DIR}
dtt config set output ${PJD_OUTPUT_DIR}

# 运行pjdtest工具，测试结果会输出到${PJD_TEST_DIR}/log目录下
dtt -t pjdtest -s pjdtest


```


### mdtest工具
主要用于测试元数据性能，测试内容包括创建文件、删除文件、重命名文件、创建目录、删除目录、重命名目录等操作，测试结果会输出到日志目录下，可以查看日志分析测试结果。
```bash

# 测试目录
MDTEST_TEST_DIR=$ARGUMENTS[0]/mdtest_test_$(date +%Y%m%d%H%M%S)
MDTEST_OUTPUT_DIR=/tmp/dtt
# 创建测试目录和日志目录
mkdir -p ${MDTEST_TEST_DIR}
mkdir -p ${MDTEST_OUTPUT_DIR}

# 配置测试目录和日志目录
dtt config set testdir ${PJD_TEST_DIR}
dtt config set output ${PJD_OUTPUT_DIR}

# 运行mdtest工具，测试结果会输出到${MDTEST_OUTPUT_DIR}/log目录下
# scene参数:
#   mdtest_z0_n100: 目录深度为0，每个目录创建100个文件
#   mdtest_z5_b4_i1: 目录深度为5，目录分支大小为4，每个目录创建1个文件
#   mdtest_z6_b3_i1: 目录深度为6，目录分支大小为3，每个目录创建1个文件
#   mdtest_z9_b2_i1: 目录深度为9，目录分支大小为2，每个目录创建1个文件
#   all: 以上所有场景都会测试
dtt -t mdtest -s $scene

```


### vdbench工具
主要用户测试数据读写性能，测试内容包括顺序读写和随机读写，测试结果会输出到日志目录下，可以查看日志分析测试结果。
```bash

# 测试目录
VDBENCH_TEST_DIR=$ARGUMENTS[0]/vdbench_test_$(date +%Y%m%d%H%M%S)
VDBENCH_OUTPUT_DIR=/tmp/dtt

# 创建测试目录和日志目录
mkdir -p ${VDBENCH_TEST_DIR}
mkdir -p ${VDBENCH_OUTPUT_DIR}

# 配置测试目录和日志目录
dtt config set testdir ${VDBENCH_TEST_DIR}
dtt config set output ${VDBENCH_OUTPUT_DIR}

# 运行vdbench工具，测试结果会输出到${VDBENCH_OUTPUT_DIR}/log目录下
# scene参数:
#  seq_wr: 顺序写测试
#  seq_rd: 顺序读测试
#  rand_wr: 随机写测试
#  rand_rd: 随机读测试
dtt -t vdbench -s $scene


```


### fio工具
主要用于测试数据读写性能，测试内容包括顺序读写和随机读写，测试结果会输出到日志目录下，可以查看日志分析测试结果。
```bash

# 测试目录
FIO_TEST_DIR=$ARGUMENTS[0]/fio_test_$(date +%Y%m%d%H%M%S)
FIO_OUTPUT_DIR=/tmp/dtt

# 创建测试目录和日志目录
mkdir -p ${FIO_TEST_DIR}
mkdir -p ${FIO_OUTPUT_DIR}

# 配置测试目录和日志目录
dtt config set testdir ${FIO_TEST_DIR}
dtt config set output ${FIO_OUTPUT_DIR}

# 运行fio工具，测试结果会输出到${FIO_OUTPUT_DIR}/log目录下
# scene参数:
#  rand_read_0d_128k_16j: 随机读测试，目录深度为0，每个目录创建128KB的文件，16个并发作业
#  rand_read_0d_128k_1j: 顺序读测试，目录深度为0，每个目录创建128KB的文件，1个并发作业
#  rand_write_0d_128k_16j: 随机写测试，目录深度为0，每个目录创建128KB的文件，16个并发作业
#  rand_write_0d_1m_16j: 顺序写测试，目录深度为0，每个目录创建1MB的文件，16个并发作业

dtt -t fio -s $scene


```


## 测试流程
1. 选择测试工具和测试场景，根据需要可以选择基础命令进行基本功能测试，也可以选择pjdtest、mdtest、vdbench、fio等工具进行更全面的功能和性能测试。
2. 根据测试工具的使用说明，配置测试目录和日志目录，并运行测试命令执行测试。
3. 测试完成后，查看日志目录下的测试结果，根据测试结果分析是否存在问题，如果测试结果不符合预期，可以根据日志信息进行排查，找出问题原因并修复代码。
4. 修复代码后，可以再次使用这个技能在开发环境进行回归测试，验证代码变更是否生效，是否引入新问题，直到测试结果符合预期为止。
5. 如果有不确定的情况，可以咨询我或者查阅相关文档，确保测试的正确性和有效性。