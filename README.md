# DingoFS

[DingoFS](https://github.com/dingodb/dingofs) is a cloud-native distributed high-speed file storage system designed and developed by [DataCanvas](https://www.datacanvas.com/). It integrates multiple features such as elasticity, multi-cloud compatibility, multi-protocol convergence, and exceptional performance.By leveraging its multi-tiered, multi-type, and high-performance distributed multi-level caching architecture, DingoFS accelerates data I/O for AI workflows, effectively addressing burst I/O challenges in AI scenarios. Additionally, it provides local cache storage capabilities to meet the full lifecycle storage requirements of large-scale AI models.


## Key Features

**1. POSIX Compliance**

DingoFS delivers a native file system-like operational experience, enabling seamless system integration.

**2. AI-Native Architecture**

Deeply optimized for large language model (LLM) workflows, efficiently managing massive training datasets and checkpoint workloads.

**3. S3 Protocol Compatibility**

DingoFS supports standard S3 interface protocols for streamlined access to filesystem namespace resources.

**4. Fully Distributed Architecture**

DingoFS's metadata Service (MDS), data storage layer, caching system, and client components all support linear scalability.

**5. Exceptional Performance**

Combines SSD-level low-latency responsiveness with object storage-grade elastic throughput capacity.

**6. Intelligent Caching Acceleration System**

DingFS implements a three-tier caching topology (memory/local SSD/distributed cluster) to deliver high-throughput, low-latency intelligent I/O acceleration for AI workloads.


## Get Start

### Docs
All Documentation [Docs](https://dingofs.readthedocs.io/en/latest/)

## 1. Installation prerequisites
### Linux Operating system
| Release    | Version     |
|----------|---------------|
| Debian   | 9+   |
| Centos   | 7+   |
| Ubuntu   | 20+  |
### Network
Deploying a cluster via DingoAdm requires the following two network prerequisites:

- The control machine where DingoAdm is installed must have SSH access to the target servers for service deployment.

- All servers must be able to pull images from the Docker registry.

## 2. Install dependencies
We recommend Rocky and Ubuntu to develop the DingoFS codebase.

### Rocky 8.9/9.3

```sh
sudo dnf install -y epel-release
sudo dnf install -y gcc-toolset-13* libnl3-devel libunwind-devel python3-devel

wget https://github.com/Kitware/CMake/releases/download/v3.30.1/cmake-3.30.1-linux-x86_64.tar.gz
tar zxvf cmake-3.30.1-linux-x86_64.tar.gz
sudo cp -rf cmake-3.30.1-linux-x86_64/bin/* /usr/local/bin/ &&   sudo cp -rf  cmake-3.30.1-linux-x86_64/share/* /usr/local/share && rm -rf cmake-3.30.1-linux-x86_64

source /opt/rh/gcc-toolset-13/enable
```
### Ubuntu 22.04/24.04
```sh
sudo apt update
sudo apt install -y make gcc g++ libnl-genl-3-dev libunwind-dev python3-dev

wget https://github.com/Kitware/CMake/releases/download/v3.30.1/cmake-3.30.1-linux-x86_64.tar.gz
tar zxvf cmake-3.30.1-linux-x86_64.tar.gz
sudo cp -rf cmake-3.30.1-linux-x86_64/bin/* /usr/local/bin/ && sudo cp -rf  cmake-3.30.1-linux-x86_64/share/* /usr/local/share && rm -rf cmake-3.30.1-linux-x86_64
```

### GCC 13
We recommend using GCC 13 as the primary compiled language.

## 3. Build DingoFS
### 1. Setup Dingo-eureka and Dingo-sdk

- [Dingo-eureka](https://github.com/dingodb/dingo-eureka): A Necessary Service Components for DingoFS.
- [Dingo-sdk](https://github.com/dingodb/dingo-sdk): A Unified Software Development Kit (SDK) required for DingoFS.

### 2. Install jemalloc
```shell
wget https://github.com/jemalloc/jemalloc/releases/download/5.3.0/jemalloc-5.3.0.tar.bz2
tar -xjvf jemalloc-5.3.0.tar.bz2
cd jemalloc-5.3.0 && ./configure && make && make install
```

 - ### Tips
If you installed the software using a [Docker](./docs/en/deploy/Docker.md) container, the container already includes pre-integrated Dingo-eureka and Dingo-sdk.

### 3. Download dep
```sh
git submodule sync
git submodule update --init --recursive
```

### 4. Build
```sh
mkdir build
cd build
cmake ..
make -j 32
```

If you want use 3FS as your block cache, you should build with `WITH_LIBUSRBIO` option:

```
cmake -DWITH_LIBUSRBIO=ON ..
```

## Special Thanks

### DataCanvas

DingoFS is Sponsored by [DataCanvas](https://www.datacanvas.com/), a new platform to do data science and data process in real-time.


DingoFS is an open-source project licensed under the **Apache License Version 2.0**, welcome any feedback from the community.
For any support or suggestion, please contact us.

## Contact us

If you have any technical questions or business needs, please contact us.

Attach the Wetchat QR Code

![](./images/dingo_contact_Wetchat.png)
