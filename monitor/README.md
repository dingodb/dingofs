# 目录结构介绍

```
monitor
├── dingo-monitor.sh        # dingo集群监控的控制脚本，用于启动、停止、重启监控功能。
├── docker-compose.yml      # 编排监控系统相关容器的配置文件，包括prometheus容器、grafana容器。
|                           # 修改该文件来配置各组件的配置参数。
├── grafana                 # grafana相关目录
│   ├── dashboards          # grafana所有dashboards的json文件存放目录，grafana将从该目录加载文件来创建dashboards；
|   |   |                   
│   │   ├── etcd.json
│   │   ├── mds.json
│   │   ├── metaserver.json
│   │   └── clinet.json
│   ├── grafana.ini         # grafana的启动配置文件，将映射到容器的 `/etc/grafana/grafana.ini` 上
│   ├── provisioning        # grafana预配置相关目录，将映射到容器的`/etc/grafana/provisioning`上
│   │   ├── dashboards
│   │   │   └── all.yml
│   │   └── datasources     # grafana的datasources的json文件存放目录，grafana将从该目录加载文件来创datasources。
|   |   └── all.yml
├── prometheus              # prometheus相关目录
│   ├── prometheus.yml      # prometheus的配置文件
│   └── target.json
├── README.md               # 安装部署指南
├── target.ini              # target_json.py脚本依赖的一些配置
├── target_json.py          # 用于生成prometheus监控对象的python脚本，每隔一段时间用dingo工具拉取监控目标并更新。
```

## 使用说明

### 环境初始化

1.部署监控系统的机器需要安装如下组件：

docker、docker-compose

* docker安装

```
$ curl -fsSL get.docker.com -o get-docker.sh
$ sudo sh get-docker.sh --mirror Aliyun
```

或者直接安装

```
ubuntu:
apt-get install docker-ce
apt-get install docker-ce-cli

Rockey linux:
sudo dnf install docker-ce 
sudo dnf install docker-ce-cli

```

* docker-compose安装

* ```
  curl -L https://github.com/docker/compose/releases/download/v2.29.1/docker-compose-`uname -s`-`uname -m` -o /usr/bin/docker-compose
  chmod +x /usr/bin/docker-compose
  ```

### 部署监控系统

* 监控系统安装

git clone https://github.com/dingodb/dingofs.git

sudo cp -r dingofs/monitor /opt/dingofs-monitor

* 监控启动和停止

假设/opt/dingofs-monitor 为监控的安装路径，可根据实际情况来进行修改。 

cd /opt/dingofs-monitor 

启动监控：

```sudo sh dingo-monitor.sh start```

停止监控：

```sudo sh dingo-monitor.sh stop```

* 监控目标的新增和删除

1.手工修改

你可以手工修改监控目标文件（prometheus/data/targets.json）来新增或者删除目标：

```
[
    {
        "labels": {
            "job": "mds"
        },
        "targets": [
            "172.20.0.10:7700",
            "172.20.0.11:7700",
            "172.20.0.12:7700"
        ]
    },
    {
        "labels": {
            "job": "metaserver"
        },
        "targets": [
            "172.20.0.10:6800",
            "172.20.0.11:6800",
            "172.20.0.12:6800",
        ]
    },
    {
        "labels": {
            "job": "etcd"
        },
        "targets": [
            "172.20.0.10:2379",
            "172.20.0.11:2379",
            "172.20.0.12:2379",
        ]
    },
    {
        "labels": {
            "job": "client"
        },
        "targets": [
            "172.20.0.10:9002",
            "172.20.0.11:9002"
        ]
    }
]
```

2.自动更新

自动更新需要依赖于dingo工具，确保已经安装好dingo工具和配置文件($HOME/.dingo/dingo.yaml)，并在PATH配置好dingo安装路径。

通过如下方式来验证安装是否成功：

```dingo --version```

然后执行脚本：

```nohup python3 target_json.py &```

target_json.py工具每隔30秒会通过dingo工具拉去集群信息，并更新targets.json文件。

新增挂载点或者卸载挂载点，都将会自动更新。

* 监控系统访问

Prometheus地址：

```
http://<ip地址>:9090
```
Grafana地址：

```
http://<ip地址>:3000
用户名：admin
密码：dingo
```