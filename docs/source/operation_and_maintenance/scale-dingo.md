Expansion Cluster 
===

Step 1: Commit host list
---

### 1. Add new machines to the host list

```shell 
$ vim hosts.yaml 
```

``` yaml 
global: 
 user: dingofs 
 ssh_port: 22 
 private_key_file: /home/dingofs/.ssh/id_rsa

private_key_file: /home/dingofs/.ssh/id_rsa
  - host: server-host1 
 hostname: 10.0.1.1
  - host: server-host2 
 hostname: 10.0.1.2
  - host: server-host3 
 hostname: 10.0.1.3
  - host: server-host4 # Add new machine 
 hostname: 10.0.1.4
  - host: server-host5 # Add new machine 
 hostname: 10.0.1.5
  - host: server-host6 # Add new machine 
 hostname: 10.0.1.6 
```

### 2. Submit the list of hosts

```shell 
$ dingoadm hosts commit hosts.yaml 
```

Step 2: Modify the cluster topology
---

Add the list of expanded services to the topology file:


```shell 
$ vim topology.yaml 
```

```yaml 
kind: dingofs 
global: 
 variable: 
 home: /tmp 
 machine1: dingo001 
 machine2: dingo002 
 machine3: dingo003 
 machine4: dingo004 # add new machine 
 machine5. dingo005 # Add new machine 
 machine6: dingo006 # Add new machine

mds_services: 
 config: 
 listen.ip: ${service_host} 
 listen.port: 6700 
 listen.dummy_port: 7700 
 deploy.
    - host: ${machine1}
    - host: ${machine2}
    - host: ${machine3}
    - host: ${machine4} # Add new service
    - host: ${machine5} # add service
    - host: ${machine6} # Add new service 
```

> :warning: **Warning:** 
> 
> * You can only scale services with the same role 
> * For the metaserver service, a new logical pool is created every time you scale up, and the new services are located in this pool.

Step 3: Expand the cluster
---

```shell 
$ dingoadm scale-out topology.yaml 
```

> :bulb: **REMINDER:** 
> 
> The scale-out operation is an idempotent operation, so the user can repeat the operation if it fails, so don't worry about the service residual problem.