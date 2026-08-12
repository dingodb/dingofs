---
name: dev-deploy
description: 在开发环境部署dingofs的技能，当开发完成功能或修复bug后，可以使用这个技能将代码部署到测试环境进行验证
context: fork
disable-model-invocation: true
---


# dingofs部署技能
**注意**: 本技能仅适用于开发环境部署测试dingofs，不能用于生产环境部署，并只进行基本功能验证，不用于性能测试或压力测试或者稳定性测试。

## 脚本
脚本在`scripts/dev-mds`目录下，包含以下文件：
- ** mds_deploy_parameters **: 部署mds服务器的参数配置文件，包含服务器实例数量、起始端口号、起始实例ID等参数。
  - CLUSTER_ID: 集群ID，默认为101
  - SERVER_NUM: 服务器实例数量，默认为1
  - SERVER_HOST: 服务器主机IP地址
  - SERVER_LISTEN_HOST: 服务器监听IP地址
  - SERVER_START_PORT: 服务器起始端口号，默认为7800
  - MDS_INSTANCE_START_ID: mds服务器实例起始ID，默认为1000
  - COORDINATOR_ADDR: coordinator地址
- ** deploy_mds.sh **: 部署mds服务器的脚本，支持部署多个实例，并且可以选择是否替换配置文件。
- ** start_mds.sh **: 启动mds服务器的脚本，支持启动多个实例，并且可以选择是否替换配置文件。
- ** stop_mds.sh **: 停止mds服务器的脚本，支持停止多个实例。
- ** start_client.sh **: 部署和启动client的脚本

## MDS部署、启动、停止
**注意**: 必须在脚本目录scripts/dev-mds下执行以下命令，否则会报错。
部署目标目录为项目根目录下的dist目录。
```bash
# 进入脚本目录
cd scripts/dev-mds

# 部署，部署目录为 dist/mds-1 dist/mds-2 ... dist/mds-N，N为服务器实例数量
# mds目录下包括：bin、conf、log等目录
bash deploy_mds.sh --server_num=${SERVER_NUM}

# 启动
bash start_mds.sh --server_num=${SERVER_NUM}

# 停止
bash stop_mds.sh --server_num=${SERVER_NUM}

# 一键部署、启动
bash clean_start.sh --server_num=${SERVER_NUM}

```


## Client部署启动
**注意**: 必须在脚本目录scripts/dev-mds下执行以下命令，否则会报错。
```bash

# 进入脚本目录
cd scripts/dev-mds

# 部署和启动client （需要先部署启动mds服务器）
# META_ADDR: mds服务器的地址，格式为mds://ip:port/fs_name 例如mds://10.220.69.5:7801/dengzh_hash_01


sudo ./start_client.sh --meta=$META_ADDR --mountpoint=$MOUNT_POINT --num=1 --noupgrade --clean_log


# 停止
sudo ./start_client.sh --meta=$META_ADDR --mountpoint=$MOUNT_POINT --num=1 --stop

```


## 步骤
1. 代码变更后，重新编译代码，编译成功后进入后续步骤。
2. 一键部署、启动mds服务器，使用clean_start.sh脚本，启动成功后进入后续步骤，可以使用`ps -ef | grep dingo-mds`命令查看mds服务器是否启动成功。
3. 先停止client，再启动client，使用start_client.sh脚本，启动成功后进入后续步骤，可以使用`ps -ef | grep dingo-client`命令查看client是否启动成功。
