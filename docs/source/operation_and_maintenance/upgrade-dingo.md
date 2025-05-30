Upgrade Service 
===

Step 1: Modify the cluster topology
---

Modify the mirror names in the cluster topology to the names of the mirrors to be upgraded:

```shell 
$ vim topology.yaml 
```

```yaml 
kind: dingofs 
global: 
 container_image: dingodatabase/dingofs:v1.2 # Modify the mirror 
...
```

Step 2: Commit the cluster topology
---

```shell 
$ dingoadm config commit topology.yaml 
```

Step 3: Upgrade the specified service
---

```shell 
$ dingoadm upgrade 
```

DingoAdm upgrades all services in the cluster by default. To upgrade a specific service, you can add the following 3 options:

* `--id`: Upgrade the service with the specified `id`.
* `--host`: Upgrade all services on the specified host.
* `--role`: Upgrade all services for the specified role.

The above 3 options can be used in any combination, and the corresponding `id`, `host`, and `role` of the services can be viewed via [dingoadm status](#View cluster status).

#### Example 1: Upgrading a service with id `c9570c0d0252

``` shell 
$ dingoadm upgrade --id c9570c0d0252 
```

#### Example 2: Upgrading all `MDS` services on the host `10.0.1.1` 
``shell 
$ dingoadm upgrade --host 10.0.1.1 --role mds 
``

> :bulb: **REMINDER:** 
> 
> `upgrade` upgrades each service specified on a rolling basis by default. Users 
> need to go into the container to determine if the cluster is healthy after upgrading each service. If you want to perform the upgrade in one go, you can add the `-f` option to force the upgrade.