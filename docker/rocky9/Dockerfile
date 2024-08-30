FROM rockylinux/rockylinux:9.3

ENV TZ=Asia/Shanghai

SHELL ["/bin/bash", "-c"]

RUN dnf update -y  \
    && dnf install -y epel-release  \
    && dnf install -y wget tar git gcc-toolset-13* perl flex bison patch fuse3-devel  libnl3-devel libunwind-devel python3-devel \
    && dnf clean all \
    && ln -fs /usr/share/zoneinfo/${TZ} /etc/localtime \
    && echo ${TZ} > /etc/timezone \
    && wget https://github.com/Kitware/CMake/releases/download/v3.30.1/cmake-3.30.1-linux-x86_64.tar.gz \
    && tar -zxvf cmake-3.30.1-linux-x86_64.tar.gz \
    && cp -rf cmake-3.30.1-linux-x86_64/bin/* /usr/local/bin/  \
    && cp -rf cmake-3.30.1-linux-x86_64/share/* /usr/local/share \
    && rm -rf cmake-3.30.1-linux-x86_64 \
    && rm -f cmake-3.30.1-linux-x86_64.tar.gz

RUN wget https://github.com/bazelbuild/bazel/releases/download/4.2.4/bazel-4.2.4-linux-x86_64 -O /usr/bin/bazel && \
    chmod a+x /usr/bin/bazel

ENV PATH=/opt/rh/gcc-toolset-13/root/usr/bin:$PATH
