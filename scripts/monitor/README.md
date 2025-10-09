# 1.目录结构介绍

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

# 2.容量规划

请确保部署位置有足够的磁盘空间来存放指标数据，这将依赖于数据采集频率及其保留策略,dingofs-monitor会定期清理过期指标数据。
目前的抓取频率是默认是5秒一次，保留策略默认为保留7天，最大使用空间是256GB。
你可以通过修改docker-compose.yml文件来定义保留策略：
```
storage.tsdb.retention.time=7d
storage.tsdb.retention.size=256GB
```
通过修改prometheus.yml文件中的scrape_interval参数修改采集频率:
```
scrape_interval:     5s # Set the scrape interval to every 15 seconds. Default is every 1 minute.
```
一般一个监控节点每5秒采集一次,每天产生的指标数据差不多200M，如果你要监控9个节点，保留14天数据的话。
容量估算可以通过如下计算公式：
```
9*200M*14d=25.2G
```

# 3.安装依赖

部署监控系统的机器需要安装如下组件：
docker、docker-compose

* docker安装
```
ubuntu:
apt-get install docker-ce
apt-get install docker-ce-cli

Rockey linux:
sudo dnf install docker-ce 
sudo dnf install docker-ce-cli
```
* docker-compose安装

 ```
  curl -L https://github.com/docker/compose/releases/download/v2.29.1/docker-compose-`uname -s`-`uname -m` -o /usr/bin/docker-compose
  chmod +x /usr/bin/docker-compose
```

# 4.部署

部署监控系统
```
git clone https://github.com/dingodb/dingofs.git
sudo cp -r dingofs/monitor /opt/dingofs-monitor <-安装部署位置可以根据实际情况来
```

# 5.监控系统的启停

假设/opt/dingofs-monitor 为监控的安装路径: 
```
cd /opt/dingofs-monitor 
```
* 启动

```
sudo sh dingo-monitor.sh start
```

* 停止

```
sudo sh dingo-monitor.sh stop
```

可以通过docker ps命令来查看是否启动或者停止成功:
```
$ sudo docker ps 
CONTAINER ID   IMAGE                                      COMMAND                  CREATED       STATUS       PORTS     NAMES
ff6f895bf9b1   grafana/grafana                            "/run.sh"                2 days ago    Up 2 days              dingofs-monitor-grafana-1
8af11c964fcf   prom/prometheus:latest                     "/bin/prometheus --c…"   2 days ago    Up 2 days              dingofs-monitor-prometheus-1
```

# 6.监控目标的新增和删除

你可以手工或者自动方式来更新监控的目标信息，这两种方式只能选择其一。
监控的目标必须来自同一个集群，暂不支持多个集群的监控。
你可以手工编辑targets.json和etcd_targets.json文件来修改监控目标信息。
其中targets.json用来修改mds、metaserver、client、remotecache的监控目标,etcd_targets.json用来
修改etcd的监控目标，这2个文件的位置在prometheus/data目录下。

自动更新会实时从集群中获取监控目标，并重新生成targets.json和etcd_targets.json文件,对于你手工修改的信息，将会被覆盖。


* 手工修改

你可以手工修改监控目标文件targets.json和etcd_targets.json来新增或者删除目标，如下所示：

targets.json:
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
            "job": "client"
        },
        "targets": [
            "172.20.0.10:9002",
            "172.20.0.11:9002"
        ]
    },
        {
        "labels": {
            "job": "remotecache"
        },
        "targets": [
            "172.20.0.13:10000",
            "172.20.0.14:10001",
            "172.20.0.15:10002"
        ]
    },
]
```

etcd_targets.json:
```
[
    {
        "labels": {
            "job": "etcd"
        },
        "targets": [
            "172.20.0.10:2379",
            "172.20.0.11:2379",
            "172.20.0.12:2379",
        ]
    }
]
```

* 自动更新

自动更新需要依赖于dingo工具，确保已经安装好dingo工具和配置文件($HOME/.dingo/dingo.yaml)，并在PATH配置好dingo安装路径。

通过如下方式来验证安装是否成功：

```
dingo version
```

然后执行脚本：

```
nohup python3 target_json.py &
```
target_json.py工具每隔30秒会通过dingo工具拉去集群信息，并更新targets.json文件，后续新增挂载点或者卸载挂载点，都将会自动更新。

# 7.监控系统的访问

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