---
name: dev-regression-test
description: 在开发环境进行回归测试，当开发完成功能或修复bug后，可以使用这个技能在开发环境进行回归测试，验证代码变更是否生效，是否引入新问题。
context: fork
disable-model-invocation: true
---


# dingofs回归测试技能
**注意**: 本技能仅适用于开发环境部署测试dingofs，不要用于生产环境部署，并只进行基本功能回归验证，提交代码之前可以使用这个技能在开发环境进行回归测试，验证代码变更是否生效，是否引入新问题。

使用方式: /dev-regression-test [测试目录路径]


## 测试环境
服务（dingo-client、dingo-mds）运行在项目目录dist下面，当出现测试问题可以查看对应日志。

目录scripts/dev-mds下面是启动和停止服务的脚本，使用方法可以参考脚本中的注释说明。

```bash

# 目录结构
dengzihui@dingofs-5 ➜  dingofs git:(feat/main_081001) tree dist
dist
├── cache
│   ├── bin
│   │   └── dingo-cache -> /home/dengzihui/work/dingofs/build/bin/dingo-cache
│   └── log
├── client
│   ├── bin
│   │   └── dingo-client -> /home/dengzihui/work/dingofs/build/bin/dingo-client
│   ├── cache
│   │   └── dengzh_hash_01-1
│   ├── conf
│   └── log
├── mds-1
│   ├── bin
│   │   ├── dingo-mds -> /home/dengzihui/work/dingofs/build/bin/dingo-mds
│   │   └── dingo-mds-client -> /home/dengzihui/work/dingofs/build/bin/dingo-mds-client
│   ├── conf
│   │   ├── coor_list
│   │   └── mds.conf
│   └── log
├── mds-2
│   ├── bin
│   │   ├── dingo-mds -> /home/dengzihui/work/dingofs/build/bin/dingo-mds
│   │   └── dingo-mds-client -> /home/dengzihui/work/dingofs/build/bin/dingo-mds-client
│   ├── conf
│   │   ├── coor_list
│   │   └── mds.conf
│   └── log
└── mds-3
    ├── bin
    │   ├── dingo-mds -> /home/dengzihui/work/dingofs/build/bin/dingo-mds
    │   └── dingo-mds-client -> /home/dengzihui/work/dingofs/build/bin/dingo-mds-client
    ├── conf
    │   ├── coor_list
    │   └── mds.conf
    └── log

```


## 回归测试工具


### 基础命令
基础的文件系统操作命令，主要用于测试基本的文件系统功能是否正常，包括创建文件、删除文件、重命名文件、创建目录、删除目录、重命名目录等操作，可以自由使用下面的工具进行测试，根据命令执行结果可以判断基本的文件系统功能是否正常。
测试方法: 使用下面的工具进行基本的文件系统操作测试，参数可以自己指定。
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
主要用于测试元数据操作是否正常，是否符合 POXIS 标准，测试内容包括创建文件、删除文件、重命名文件、创建目录、删除目录、重命名目录等操作。
测试方法: 使用prove命令运行pjdfstest目录下的测试用例，参数可以自己指定，注意必须用sudo权限。
```bash

# 环境信息
PJD_SUFFIX=$(date +%Y%m%d%H%M%S)
PJD_TEST_DIR=$ARGUMENTS[0]/pjd_test_${PJD_SUFFIX}
PJD_LOG_DIR=/tmp/dev-regression-test/pjd_test_${PJD_SUFFIX}



# 创建测试目录和日志目录
mkdir -p ${PJD_TEST_DIR}
mkdir -p ${PJD_LOG_DIR}

# 必须跳转到测试目录
cd ${PJD_TEST_DIR} 

# 注意：必须用sudo权限运行测试用例，否则会出现权限问题，导致测试失败。

# 示例: 运行全部测试用例
sudo prove -rv --exec 'bash -x' /home/dengzihui/work/dingofs-test/pjdfstest/tests > $PJD_LOG_DIR/pjd_test.log 2>&1

# 示例: 运行部分测试用例
sudo prove -rv --exec 'bash -x' /home/dengzihui/work/dingofs-test/pjdfstest/tests/mknod > $PJD_LOG_DIR/pjd_test.log 2>&1

# 示例: 运行一个测试用例
sudo prove -rv --exec 'bash -x' /home/dengzihui/work/dingofs-test/pjdfstest/tests/mknod/00.t > $PJD_LOG_DIR/pjd_test.log 2>&1


```

### fsx工具

```bash

# 环境信息
FSX_SUFFIX=$(date +%Y%m%d%H%M%S)
FSX_TEST_FILE=$ARGUMENTS[0]/fsx_test_${FSX_SUFFIX}
FSX_LOG_DIR=/tmp/dev-regression-test/fsx_test_${FSX_SUFFIX}


# 创建测试目录和日志目录
mkdir -p ${FSX_TEST_FILE}
mkdir -p ${FSX_LOG_DIR}


# 示例: 运行测试命令
fsx -l 1073741824 -o 1048576 -S 0 -p 10000 --duration=3600 --record-ops=$LOG_DIR/fsx.ops -P $FSX_LOG_DIR $FSX_TEST_FILE

```

### mdtest工具
主要用于测试元数据性能，测试内容包括创建文件、删除文件、重命名文件、创建目录、删除目录、重命名目录等操作，测试结果会输出到日志目录下，可以查看日志分析测试结果。
测试方法: 使用mpirun和mdtest工具结合起来测试,参数可以自己指定。
```bash

# 环境信息
MDTEST_SUFFIX=$(date +%Y%m%d%H%M%S)
MDTEST_TEST_DIR=$ARGUMENTS[0]/mdtest_test_${MDTEST_SUFFIX}
MDTEST_LOG_DIR=/tmp/dev-regression-test/mdtest_test_${MDTEST_SUFFIX}

# 创建测试目录和日志目录
mkdir -p ${MDTEST_TEST_DIR}
mkdir -p ${MDTEST_LOG_DIR}

# 示例: 运行测试命令
mpirun -np 4 mdtest -z 0 -b 1 -n 1000 -L -C -F -d ${MDTEST_TEST_DIR} > ${MDTEST_LOG_DIR}/mdtest.log 2>&1

```


### vdbench工具
主要用户测试数据读写性能，测试内容包括顺序读写和随机读写，测试结果会输出到日志目录下，可以查看日志分析测试结果。
测试方法: 使用vdbench工具进行测试，参数可以自己指定。
```bash

# 环境信息
VDBENCH_SUFFIX=$(date +%Y%m%d%H%M%S)
VDBENCH_TEST_DIR=$ARGUMENTS[0]/vdbench_test_${VDBENCH_SUFFIX}
VDBENCH_TOOL_DIR=/home/dengzihui/work/dingofs-test/vdbench
VDBENCH_LOG_DIR=/tmp/dev-regression-test/vdbench_test_${VDBENCH_SUFFIX} 


# 创建测试目录和日志目录
mkdir -p ${VDBENCH_TEST_DIR}
mkdir -p ${VDBENCH_LOG_DIR}



# 跳转到工具目录
cd ${VDBENCH_TOOL_DIR}

# 注意: ${VDBENCH_TOOL_DIR}/config目录下有一些测试配置，可以根据需要修改配置文件，或者自己创建新的配置文件进行测试，特别注意要修改配置中的目标测试目录

# 示例: 运行测试命令
./vdbench -f config/test-01.vd > ${VDBENCH_LOG_DIR}/vdbench.log 2>&1


```


### fio工具
主要用于测试数据读写性能，测试内容包括顺序读写和随机读写，测试结果会输出到日志目录下，可以查看日志分析测试结果。
测试方法: 使用fio工具进行测试，参数可以自己指定。
```bash

# 环境信息
FIO_SUFFIX=$(date +%Y%m%d%H%M%S)
FIO_TEST_DIR=$ARGUMENTS[0]/fio_test_${FIO_SUFFIX}
FIO_LOG_DIR=/tmp/dev-regression-test/fio_test_${FIO_SUFFIX}


# 创建测试目录
mkdir -p ${FIO_TEST_DIR}
mkdir -p ${FIO_LOG_DIR}

# 跳转到测试目录
cd ${FIO_TEST_DIR}

# 示例: 运行测试命令
fio --ioengine=libaio --iodepth=1  --direct=1 --rw=read --bs=128KB --size=8GB --numjobs=32 --group_reporting --name=test --log-file=${FIO_LOG_DIR}/fio.log

```

### fsstress工具
主要进行文件系统压力测试和并发测试
测试方法: 使用fsstress工具进行测试，参数可以自己指定。
```bash

# 环境信息
FSSTRESS_SUFFIX=$(date +%Y%m%d%H%M%S)
FSSTRESS_TEST_DIR=$ARGUMENTS[0]/fsstress_test_${FSSTRESS_SUFFIX}
FSSTRESS_LOG_DIR=/tmp/dev-regression-test/fsstress_test_${FSSTRESS_SUFFIX}

# 创建测试目录和日志目录
mkdir -p ${FSSTRESS_TEST_DIR}
mkdir -p ${FSSTRESS_LOG_DIR}

# 跳转到测试目录
cd ${FSSTRESS_TEST_DIR}

# 示例: 运行测试命令
/opt/ltp/testcases/bin/fsstress -d ${FSSTRESS_TEST_DIR} -n 10000 -p 8 -v > ${FSSTRESS_LOG_DIR}/fsstress.log 2>&1

```

### xfstests工具
主要用于测试文件系统语义。
测试方法: 项目下有xfstests目录，里面有xfstests说明信息，可以参考。
```bash

# 环境信息
# xfstests项目目录
XFSTESTS_TOOL_DIR=/home/dengzihui/work/dingofs-test/xfstests-dev

# MDS地址
DINGOFS_META_URL_TEMPLATE=mds://10.220.69.5:7801/{fsname}

# 准备好环境后，运行测试用例，只运行generic下面的已支持的测试用例
# 已支持的测试用例文件: $XFSTESTS_TOOL_DIR/tests/generic/supported


```


## 测试流程
1. 除非指定具体工具名称，否则按顺序运行所有工具，遇到错误则停止运行。
2. 先学习测试工具，再使用工具进行测试。
3. 每个工具跑完之后，查看测试结果，如果测试结果不符合预期，可以根据日志信息进行排查，找出问题原因并尝试给出修复方案，等待确认。
4. 如果有不确定的情况，可以咨询我，确保测试的正确性和有效性。