# Use Docker Image

We provide docker images of different operating systems to support compilation and development.including

- rocky8.9
- rocky9.3
- ubuntu22.04
- ubuntu24.04

## Build docker image

For different operating systems, we can refer to the following commands to build

``````
cd dingofs 

docker build docker/rocky8/  -t dingofs-rocky-8.9-dev

docker build docker/rocky9/  -t dingofs-rocky-9.3-dev

docker build docker/ubuntu22/  -t dingofs-ubuntu-22.04-dev

docker build docker/ubuntu24/  -t dingofs-ubuntu-24.04-dev

``````
