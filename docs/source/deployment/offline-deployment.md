# Offline Deployment 
## Offline environment preparation
### DingoFS image preparation 
Download the official DingoFS image (e.g. `dingodatabase/dingofs:latest`) to the local environment (machine with access to the extranet) 
```bash 
$ sudo docker pull dingodatabase/dingofs:latest 
```
### Package the image and export it to the local repository node.

- Package the image 
```bash
# View the downloaded DingoFS image 
$ sudo docker image ls 
dingodatabase/dingofs latest ac55b3c269f7 11 days ago 3.2GB

# Package the image 
$ sudo docker save -o dingofs_v2.4.tar ac55b3c269f7 
```
- Copy image to local repository node 
```bash 
$ scp dingofs_v2.4.tar ${desthost}:/path/to/save/image 
```
- Export the image

```bash 
$ docker load --input dingofs_v2.4.tar

# View exported image 
$ sudo docker image ls 
dingodatabase/dingofs v2.4 ac55b3c269f7 11 days ago 3.2GB 
```

### Local image repository setup

Use docker-registry to build the local repository. After building the local repository, upload the DingoFS image you downloaded in the previous steps to the local repository. There are several steps to do this:

- Run docker-registry 
```bash 
$ docker run -d -p 5000:5000 --restart=always --name registry registry 
```
- Marking DingoFS images

Mark the downloaded DingoFS image, for example, mark the downloaded image (`dingodatabase/dingofs:v2.4`) as `127.0.0.1:5000/dingofs:v2.4_local` (where `127.0.0.1` is the IP of the local repository service and `5000` is the port number of the local repository service). port number, please modify according to the actual environment)

```bash
# View the downloaded DingoFS image 
$ sudo docker image ls 
dingodatabase/dingofs v2.4 5717f16d4bec 1 months ago 1.84GB

# Tag the image 
$ sudo docker tag dingodatabase/dingofs:v2.4 127.0.0.1:5000/dingofs:v2.4_local

# View the tagged image 
$ sudo docker image ls 
 127.0.0.1:5000/dingofs v2.4_local 5717f16d4bec 13 months ago 1.84GB 
```
- Upload image 
```bash 
$ docker push 127.0.0.1:5000/dingofs:v2.4_local 
```

For more details, see [private repository build](https://yeasy.gitbook.io/docker_practice/repository/registry)

### Modify the mirror address 
Modify the mirror address configuration item (`container_image`) in the client deployment configuration file `client.yaml` and the server cluster deployment configuration file `topology.yaml` to the local repository mirror address (e.g., `127.0.0.1:5000/dingofs:v2.4_ local`)

## Deploy
### Install DingoAdm 
DingoAdm is a Dingofs deployment tool, machines with extranets can install it with one click, see [DingoAdm Installation](../dingoadm/install.md) for specific installation 89%E8%A3%85-dingoadm)

However, since this article is about deployment in an intranet environment, you need to follow the steps below:
- Download DingoAdm to a locally accessible extranet machine.
- Download DingoAdm to a locally accessible machine on the extranet, and copy DingoAdm to the host machine where the DingoFS cluster is to be deployed.
- Unpack DingoAdm.
- Copy the executable and set the environment variables.

```bash 
$ mv dingoadm ~/.dingoadm

## Consider updating to ~/.bash_profile for persistence 
$ export PATH=~/.dingoadm/bin:$PATH 
```

### Host Configuration

Configure a list of servers to be used by the DingoFS cluster and submit the list to DingoAdm for management. The host configuration process is relatively simple, just add the actual hostname and ip to `hosts.yaml` and submit it.

Refer to the documentation for specific configuration: [host management](../dingoadm/hosts.md)

### DingoFS server-side deployment

You need to modify the mirror in `topology.yaml` to be the local mirror address, the example is as follows:

``` yaml 
kind: dingofs 
global: 
 container_image: 127.0.0.1:5000/dingofs:v2.4_local ## Modify to local mirror 
``` 
For other configuration items, please refer to the documentation [DingoFS cluster deployment](./dingofs-cluster-deployment.md)
### Client-side deployment 
You need to modify the image in `client.yaml` to be the local image address, example is as follows: 
```
kind: dingofs 
global: 
 container_image: 127.0.0.1:5000/dingofs:v2.4_local ## Modify to be the local mirror 
```

For other configuration items, please refer to the documentation [Deploying DingoFS Client](./dingofs-client-deployment.md).