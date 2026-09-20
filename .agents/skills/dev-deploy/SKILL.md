---
name: dev-deploy
description: 开发环境部署/重部署 dingofs：部署并启动 MDS 与 client，再做基本可用性冒烟。开发完成功能或修复 bug 后，需要把改动跑起来（重启服务）验证时使用。仅限开发环境，不做性能/压力/稳定性测试。
context: fork
---

# dingofs 部署技能

仅适用于开发环境：部署 + 基本功能验证。不用于生产环境，也不用于性能/压力/稳定性测试。

## 参数

`scripts/dev-mds/mds_deploy_parameters.local` —— 本机私有，已被 `.gitignore` 忽略；入库的模板是 `mds_deploy_parameters`，首次从模板复制。

脚本真正消费的键：

- `SERVER_NUM`：MDS 实例数
- `SERVER_HOST` / `SERVER_LISTEN_HOST`：对外 / 监听地址
- `SERVER_START_PORT`：起始端口；第 `i` 个实例为 `SERVER_START_PORT + i`，即首实例 `7801`
- `CLUSTER_ID`、`MDS_INSTANCE_START_ID`、`COORDINATOR_ADDR`
- `STORAGE_ENGINE` / `STORAGE_URL`：由 `deploy_mds.sh` 写进 `mds.conf`
- `S3_ENDPOINT` / `S3_AK` / `S3_SK` / `S3_BUCKETNAME`、`LOCAL_DATASTORE_PATH`：仅 `create_fs.sh` 使用

## 步骤

以下命令**必须**在 `scripts/dev-mds` 目录下执行，否则脚本会报错。部署产物在项目根目录 `dist/`。

1. **编译**

   ```bash
   cd build && make -j 12
   ```

   判据：退出码 0。

2. **停止 client**

   先停 client 再动 MDS —— MDS 的二进制会被重新软链，挂着旧进程容易踩坑。

   ```bash
   cd scripts/dev-mds
   sudo ./start_client.sh --meta=$META_ADDR --mountpoint=$MOUNT_POINT --num=1 --stop
   ```

3. **部署并启动 MDS**

   ```bash
   bash clean_start.sh --server_num=$SERVER_NUM
   ```

   判据：`pgrep -c dingo-mds` 输出等于 `SERVER_NUM`。（不要用 `ps -ef | grep`，它会匹配到自己，也不校验数量。）

   失败看 `dist/mds-<i>/log/out`。

4. **启动 client**

   `META_ADDR` 格式为 `mds://<SERVER_HOST>:<SERVER_START_PORT+1>/<fs_name>`，例如 `mds://10.220.69.5:7801/dengzh_hash_01`。fs 不存在时先创建：

   ```bash
   bash create_fs.sh --fs_name=$FS_NAME --mds_addr=$SERVER_HOST:$(($SERVER_START_PORT + 1))
   ```

   ```bash
   sudo ./start_client.sh --meta=$META_ADDR --mountpoint=$MOUNT_POINT \
     --num=1 --noupgrade --clean_log
   ```

   - `--noupgrade`：不重装 client 二进制，直接用 `dist/client/bin` 下已有的
   - `--clean_log`：清掉 `dist/client/log/` 的旧日志（排查问题时建议保留，去掉此参数）

   判据：`mountpoint -q $MOUNT_POINT`。

5. **冒烟验证**

   ```bash
   touch $MOUNT_POINT/.deploy_check && rm $MOUNT_POINT/.deploy_check
   ```

   判据：退出码 0。失败先看 `dist/client/log/` 与 `dist/mds-*/log/`。

6. **汇报**

   给用户：MDS 实例数、client 挂载点、冒烟结果、日志路径。**不要自行 git 提交**，提交交给用户或 `/skill:git-commit`。

需要跑测试（单元 / e2e / 重型回归 / xfstests）→ `/skill:automate-test`。

## 首次环境

集群和文件系统尚不存在时，MDS 起来之后先建（`create_cluster.sh` 的 `--cluster_id` 必须大于 0）：

```bash
bash create_cluster.sh --cluster_id=101
bash create_fs.sh --fs_name=$FS_NAME --mds_addr=$SERVER_HOST:$(($SERVER_START_PORT + 1))
```

## 其他脚本

`deploy_mds.sh` / `start_mds.sh` / `stop_mds.sh` 是 `clean_start.sh` 的拆分，只重启 MDS 时单独用。参数以 `bash <脚本> --help` 为准。
