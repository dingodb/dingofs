# What is DingoFS

## Introduction

[DingoFS](https://github.com/dingodb/dingofs) is an open source, cloud-native, high-performance distributed file system developed by [DataCanvas](https://www.datacanvas.com/), featuring elasticity, multi-cloud adaptability, multi-protocol compatibility, and high throughput. It leverages a multi-layer, multi-type, and high-speed distributed caching system to accelerate data I/O in AI workflows, effectively addressing burst I/O demands in AI scenarios. Additionally, it provides local caching capabilities to support full-lifecycle storage requirements for large AI models.

## Key Features

**1.POSIX Compliance**

Offers a local file system-like user experience to enable seamless system integration.

**2.AI-Native Architecture**

Deeply optimized for large language model (LLM) workflows, enabling efficient handling of massive training datasets and checkpoint workloads.

**3.S3 Protocol Compatibility**

Supports the standard S3 interface protocol for easy access to file system namespaces.

**4.Fully Distributed Architecture**

The Metadata Service (MDS), data storage layer, caching system, and client components all support linear scaling.

**5.Exceptional Performance**

Combines local SSD-level low latency with object storage-level high throughput, meeting the needs of both high-performance computing and large-capacity storage.

**6.Intelligent Cache Acceleration System**

Implements a 3-tier cache hierarchy (memory, local SSD, distributed cluster) with dynamic data path optimization to provide high-throughput, low-latency I/O acceleration for compute-intensive AI workloads.

## Use Cases

**Enabling AI Model Training**

In AI/training scenarios, distributed file systems need to efficiently manage massive training data (e.g., images, text, audio, etc.) and provide high-throughput, low-latency access.DingoFS accelerates small-file reads and reduces data-processing den waiting time through optimized metadata management and data distribution strategies. Meanwhile, its POSIX-compatible interface significantly reduces the complexity of data preprocessing and loading.

**High Performance Computing (HPC)**

Distributed file systems provide highly aggregated bandwidth and low-latency access in scenarios where the return of computation results is critical. DingoFS improves read and write performance of large files through data sharding and parallel I/O optimization, and its elastic scalability dynamically adjusts storage resources with the size of the computing cluster to meet the high load demands of today's market.

**High-Frequency Data Analytics**

In use cases such as quantitative trading and risk modeling, DingoFS leverages in-memory caching to enable microsecond-level data access. It supports real-time processing of massive market datasets, ensuring zero-latency response for high-frequency trading systems.