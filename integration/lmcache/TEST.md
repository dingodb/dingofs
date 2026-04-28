需要我们把 DingoFSConnector 部署到实际的 GPU 环境镜像测试：
* 分布式缓存：jg29, jg31
* DingoFSConnector: a100

> 如果不知道怎么连 a100，可以看下本项目的 memory


# 1. 部署分布式缓存

## 关于环境
* `jg29`、`jg30` 这 2 台机器有 2 块 NVMe 盘可以用作缓存盘:
    * `/mnt/cache0`
    * `/mnt/cache1`
* 我自己部署的脚本都放在上述 2 台机器的 `/home/wine93/deploy/dingo-cache/start_node.sh`，你可以作为参考。里面有日志路径，有 MDS 地址等。

## 关于部署
* 我一般在 `jg29`、`jg30` 这 2 台机器上各启一个缓存节点，每个节点管理上述的 2 块盘


## 2. 编译 DingoFSConnector

我在 a100 机器上把 dingofs 的项目这些都弄好了，你需要 fetch 到本地的这个分支，进行编译 DingoFSConnector

* 编译前需要进入指定的 python 环境：

```bash
cd /dingofs/data/venv_vllm
source .venv/bin/activate
```

* dingofs 目录在 /root/wine93，你需要进入这个目录编译 DingoFSConnector，并进行安装

* 需要注意的是，你访问 github 这些可能需要代理，以下这个代理是可以使用的：10.220.88.31:1080

# 3. 运行 vllm server

脚本配置都在 /dingofs/data/venv_vllm/wine93/ 目录下。

* 需要进入指定的 python 环境，如果已经进过了就忽略

```bash
cd /dingofs/data/venv_vllm
source .venv/bin/activate
```

* 运行 vllm server

```bash
cd /dingofs/data/venv_vllm/wine93/bin/
bash vllm_deepseek_for_connector.sh
```

待其成功启动后，再运行 bench


# 4. 运行 vllm bench

```bash
cd /dingofs/data/venv_vllm/wine93/bin/
bash bench_deepseek.sh
```

注意：
* 记得收集压测结果，vllm bench 的结果需要保存到文件

