Deploying a DingoFS cluster using DingoAdm
===

Step 1: Prepare the environment
---

* [Hardware and software requirements](install-dingoadm#Hardware-and-software-requirements)
* [Install dependencies](install-dingoadm#Install-dependencies)

Step 2: Install DingoAdm on the control machine
---

* [Install DingoAdm](install-dingoadm#install-dingoadm)


Step 3: Import the host list
---

Users need to import the list of machines required for deploying the cluster and clients so that they can fill in the hostnames of the deployed services in various configuration files later.
Please ensure that all hostnames appearing in various configuration files later have been imported. For details, see [Host Management][hosts].

### 1. Prepare the host list

```shell
$ vim hosts.yaml
```

```yaml
global:
  user: dingo
  ssh_port: 22
  private_key_file: /home/dingo/.ssh/id_rsa

hosts:
  - host: server-host1
    hostname: 10.0.1.1
  - host: server-host2
    hostname: 10.0.1.2
  - host: server-host3
    hostname: 10.0.1.3
  - host: client-host
    hostname: 10.0.1.4
```

### 2. Import the host list
```shell
$ dingoadm hosts commit hosts.yaml
```

Step 4: Prepare the cluster topology file
---

We have prepared different topology file templates based on common scenarios. Users can select and edit them according to their needs:

* [Single-machine deployment][dingofs-stand-alone-topology]

All services run on a single host, typically used for testing or evaluation.


* [Multi-node deployment][dingofs-cluster-topology]

  A generic multi-node deployment template suitable for production environments or testing

For details on the configuration options in the topology file, please refer to [DingoFS Cluster Topology][dingofs-topology]

```shell
$ vim topology.yaml
```

```yaml
kind: dingofs
global:
  container_image: dingodatabase/dingofs:latest
  log_dir: ${home}/dingofs/logs/${service_role}
  data_dir: ${home}/dingofs/data/${service_role}
  variable:
    home: /tmp
    machine1: server-host1
    machine2: server-host2
    machine3: server-host3

etcd_services:
  config:
    listen.ip: ${service_host}
    listen.port: 2380
    listen.client_port: 2379
  deploy:
    - host: ${machine1}
    - host: ${machine2}
    - host: ${machine3}

mds_services:
  config:
    listen.ip: ${service_host}
    listen.port: 6700
    listen.dummy_port: 7700
  deploy:
    - host: ${machine1}
    - host: ${machine2}
    - host: ${machine3}

metaserver_services:
  config:
    listen.ip: ${service_host}
    listen.port: 6800
    listen.external_port: 7800
    metaserver.loglevel: 0
  deploy:
    - host: ${machine1}
    - host: ${machine2}
    - host: ${machine3}
      config:
        metaserver.loglevel: 3
```

Step 5: Add a cluster and switch clusters
---

#### 1. Add the 'my-cluster' cluster and specify the cluster topology file

```shell
$ dingoadm cluster add my-cluster -f topology.yaml
```

#### 2. Switch the 'my-cluster' cluster to the current management cluster

```shell
$ dingoadm cluster checkout my-cluster
```

Step 6: Deploy the cluster
---

```shell
$ dingoadm deploy
```

If the deployment is successful, it will output something like `Cluster 'my-cluster' successfully deployed ^_^.`.

> 📢 **Note:**
>
> By default, the deployment process runs the [precheck module][precheck] to detect factors that may cause deployment failure in advance, thereby improving the success rate of deployment. If the precheck fails, users need to troubleshoot the issue step by step based on the reported [error code][errno] and the provided solutions, and ultimately pass all prechecks. Of course, users can also skip the precheck by adding the `-k` option, but we strongly discourage this, as it may leave potential issues for future deployments and create difficult-to-troubleshoot problems.

Step 7: View the cluster status
---

```shell
$ dingoadm status
```

DingoAdm will display the service ID, service role, host address, number of deployed replica services, container ID, and running status by default:

```shell
Get Service Status: [OK]

cluster name      : my-cluster
cluster kind      : dingofs
cluster mds addr  : 10.0.1.1:6700,10.0.1.2:6700,10.0.1.3:6700
cluster mds leader: 10.0.1.1:6700 / 505da008b59c

Id            Role        Host          Replicas  Container Id  Status
--            ----        ----          -------   ------------  ------
c9570c0d0252  etcd        server-host1  1/1       ced84717bf4b  Up 45 hours
493b7831907c  etcd        server-host2  1/1       907f8b84f527  Up 45 hours
8438cc5ecb52  etcd        server-host3  1/1       44eca4798424  Up 45 hours
505da008b59c  mds         server-host1  1/1       37c05bbb39af  Up 45 hours
e7bfb934182b  mds         server-host2  1/1       044b56281928  Up 45 hours
1b322781339c  mds         server-host3  1/1       b00481b9872d  Up 45 hours
2912bbdbcb48  metaserver  server-host1  1/1       8b7a14b872ff  Up 45 hours
b862ef6720ed  metaserver  server-host2  1/1       8e2a4b9e16b4  Up 45 hours
ed4533e903d9  metaserver  server-host3  1/1       a35c30e3143d  Up 45 hours
```

* To view additional information, such as listening ports, log directories, data directories, etc., add the `-v` parameter
* For [replicas] services on the same host, their status is collapsed by default. Add the `-s` parameter to display each replica service

Step 8: Verify cluster health status
---

A cluster service running normally does not necessarily mean the cluster is healthy, so we have built the `dingofs_tool` tool into each container.
This tool not only queries the cluster's health status but also provides many other features, such as displaying detailed status of each component, creating/deleting file systems, etc.

First, we need to enter any service container (the service ID can be viewed via `dingoadm status`):

```shell
$ dingoadm enter <Id>
```

Execute the following command within that container to check:
```shell
$ dingofs_tool status
```

If the cluster is healthy, the output will include the message `cluster is healthy` at the end.

[hosts]: ../hosts.md
[important-config]: ../topology.md
[dingofs-stand-alone-topology]: ./stand-alone-topology.yaml
[dingofs-cluster-topology]: ./cluster-topology.yaml
[dingofs-topology]: ../topology.md
[precheck]: ./precheck.md
[errno]: ../errno.md
[replicas]: ../topology#replicas