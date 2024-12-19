# DINGOFS

## What's dingofs

DingoFS is a project fork from Curve. 

Curve is a sandbox project hosted by the CNCF Foundation. It's cloud-native, high-performance, and easy to operate. Curve is an open-source distributed storage system for block and shared file storage.

## Requirements

gcc 13

[dingo-eureka](https://github.com/dingodb/dingo-eureka)

## Dependencies

### Rocky 8.9/9.3

```sh
sudo dnf install -y epel-release
sudo dnf install -y gcc-toolset-13* fuse3-devel  libnl3-devel libunwind-devel python3-devel

wget https://github.com/Kitware/CMake/releases/download/v3.30.1/cmake-3.30.1-linux-x86_64.tar.gz
tar zxvf cmake-3.30.1-linux-x86_64.tar.gz
sudo cp -rf cmake-3.30.1-linux-x86_64/bin/* /usr/local/bin/ &&   sudo cp -rf  cmake-3.30.1-linux-x86_64/share/* /usr/local/share && rm -rf cmake-3.30.1-linux-x86_64

source /opt/rh/gcc-toolset-13/enable
```

### Ubuntu 22.04/24.04

```sh
sudo apt update
sudo apt install -y make gcc g++ libnl-genl-3-dev libunwind-dev libfuse3-dev python3-dev

wget https://github.com/Kitware/CMake/releases/download/v3.30.1/cmake-3.30.1-linux-x86_64.tar.gz
tar zxvf cmake-3.30.1-linux-x86_64.tar.gz
sudo cp -rf cmake-3.30.1-linux-x86_64/bin/* /usr/local/bin/ && sudo cp -rf  cmake-3.30.1-linux-x86_64/share/* /usr/local/share && rm -rf cmake-3.30.1-linux-x86_64
```

## How to build 

### Build dingo-eureka

refer https://github.com/dingodb/dingo-eureka


### Install jemalloc
```shell
wget https://github.com/jemalloc/jemalloc/releases/download/5.3.0/jemalloc-5.3.0.tar.bz2
tar -xjvf jemalloc-5.3.0.tar.bz2
cd jemalloc-5.3.0 && ./configure && make && make install
```

### Build Etcd Client

```sh
bash build_thirdparties.sh
```

### Build 
```sh
mkdir builld
cd build
cmake ..
make -j 32
```

## Build tools-v2

### Dependencies

#### Install protobuf

```sh
PB_REL="https://github.com/protocolbuffers/protobuf/releases"
curl -LO $PB_REL/download/v25.1/protoc-25.1-linux-x86_64.zip
unzip protoc-25.1-linux-x86_64.zip -d $HOME/.local
export PATH="$PATH:$HOME/.local/bin"
```

#### Install musl-gcc

```shell
wget https://musl.libc.org/releases/musl-1.2.5.tar.gz
tar -xzvf musl-1.2.5.tar.gz
cd musl-1.2.5 && sudo ./configure && sudo make install
export PATH=$PATH:/usr/local/musl/bin
```

#### Install golang

```shell
wget https://go.dev/dl/go1.23.0.linux-amd64.tar.gz
tar -C /usr/local -xzf go1.23.0.linux-amd64.tar.gz
export PATH=$PATH:/usr/local/go/bin
```

### Build 
```sh
cd tools-v2

make build
```
