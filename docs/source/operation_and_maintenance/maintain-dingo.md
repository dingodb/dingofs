# DingoAdm Common Operations

## Viewing the Cluster List

```shell
$ dingoadm cluster ls
```

To display cluster details, add the `-v` option, which displays information about the `cluster ID`, `cluster UUID`, `cluster creation time`, `cluster description`, and so on:

```shell
$ dingoadm cluster ls -v
```

## Switching clusters

Switches the specified cluster to the current managed cluster. After switching a cluster, all subsequent operations will work on that cluster.

```shell
$ dingoadm cluster checkout < cluster-name>
```

After switching clusters, when you look at the cluster list again, a `*` icon will appear in front of the current cluster name, which we use to identify the current operating cluster.

## Add Cluster

Users can specify the cluster topology file while adding a cluster, or they can add a cluster first and then submit the cluster topology by [Modify Cluster Topology](#Modify_Cluster_Topology):

```shell
$ dingoadm cluster add < cluster-name> [-f topology.yaml]
```

## Delete Cluster

```shell
$ dingoadm cluster rm <cluster-name>
```

> ⚠️ **Warning:**
>
> After deleting a cluster, all information related to the cluster will be erased, please be careful. `dingoadm` supports managing multiple clusters at the same time, please do not delete a cluster if it is not necessary.

## Export Cluster

We can save the current cluster information as a local file, usually we need to export the cluster in the following 2 cases:
*  Cluster information needs to be backed up on a regular basis to prevent its loss
*  Sharing cluster information with other users for other users to operate the cluster

```shell
$ dingoadm cluster export <cluster-name> [-o database-file-path]
```

> 💡 **Reminder:**
>
> The exported cluster file is keyed by the cluster `UUID`, which is globally unique, and the value holds the following 2 types of information:
> *  Cluster service configuration, i.e., cluster topology
> *  Information about each service, including the service ID, the container ID on which the service is running, etc.

## Import Cluster

```shell
$ dingoadm cluster import < cluster-name> [-f database-file-path]
```

> 📢 **Note:**
> *  The cluster name specified when importing a cluster must not duplicate an existing cluster name, otherwise the import will fail
> *  Importing a cluster imports the cluster topology intact, so some configuration items may be invalidated, such as the SSH private key path configuration item `private_key_path`.
> In this case, users can modify the cluster topology and commit to operate the cluster normally.

## Viewing Cluster Topology

```shell
$ dingoadm config show
```

> 💡 **Reminder:**
>
> When the local topology file is lost, we can recover it by saving the current cluster topology:
> ```shell
> $ dingoadm config show > topology.yaml
> ```

## Modifying the Cluster Topology
<a id="Modify_Cluster_Topology"></a>

Modify the local cluster topology:

```shell
$ vim topology.yaml
```

Once we have modified our local topology file, we need to submit our changes to DingoAdm for the cluster topology to take effect:

```shell
$ dingoadm config commit < topology.yaml>
```

> 💡 **Reminder:**
>
> When submitting the local cluster topology, the terminal will display the difference between the local cluster topology and the current cluster topology, so please compare carefully to prevent incorrect submission.

## Comparing Cluster Topologies

We can view the differences between the local cluster topology file and the current cluster topology by using the following command:

```shell
$ dingoadm config diff < topology.yaml>
```

## Viewing Cluster Status

```shell
$ dingoadm status
```

DingoAdm displays the Service ID, Service Role, Hostname, Number of Deployed Replication Services, Container ID, and Running Status by default:

```shell
Get Service Status: [OK]

cluster name      : my-cluster
cluster kind      : dingofs
cluster mds addr  : 10.0.1.1:6666,10.0.1.2:6666,10.0.1.3:6666
cluster mds leader: 10.0.1.1:6666 / 505da008b59c

Id            Role           Host          Replicas  Container Id  Status
--            ----           ----          -------   ------------  ------
c9570c0d0252  etcd           server-host1  1/1       ced84717bf4b  Up 45 hours
493b7831907c  etcd           server-host2  1/1       907f8b84f527  Up 45 hours
8438cc5ecb52  etcd           server-host3  1/1       44eca4798424  Up 45 hours
505da008b59c  mds            server-host1  1/1       37c05bbb39af  Up 45 hours
e7bfb934182b  mds            server-host2  1/1       044b56281928  Up 45 hours
1b322781339c  mds            server-host3  1/1       b00481b9872d  Up 45 hours
<replicas>    chunkserver    server-host1  3/3       <replicas>    RUNNING
<replicas>    chunkserver    server-host2  3/3       <replicas>    RUNNING
<replicas>    chunkserver    server-host3  3/3       <replicas>    RUNNING
2912bbdbcb48  snapshotclone  server-host1  1/1       8b7a14b872ff  Up 45 hours
b862ef6720ed  snapshotclone  server-host2  1/1       8e2a4b9e16b4  Up 45 hours
ed4533e903d9  snapshotclone  server-host3  1/1       a35c30e3143d  Up 45 hours
```

* If you want to see the rest of the information, such as listening ports, log directories, data directories, etc., you can add the `-v` parameter.
*  For [replicas][replicas] services on the same host, the status is collapsed by default, and the `-s` parameter can be added to display each replica service

## Starting services

```shell
$ dingoadm start
```

DingoAdm starts all services in the cluster by default. If you want to start a specific service, you can do so by adding the following 3 options:

*  `--id': Starts the service with the specified `id'.
*  `--host`: Starts all services on the specified host.
*  `--role`: Starts all services in the specified role.

The above 3 options can be used in any combination, and the corresponding `id`, `host`, and `role` of the service can be viewed via [dingoadm status](#Client_Status).

#### Example 1: Starting service with id `c9570c0d0252`

```shell
$ dingoadm start --id c9570c0d0252
```

#### Example 2: Starting All `MDS` Services on the `10.0.1.1` Host
```shell
$ dingoadm start --host 10.0.1.1 --role mds
```

## Stop services
<a id="Discontinuation"></a>

```shell
$ dingoadm stop
```

DingoAdm stops all services in the cluster by default, if you want to stop a specific service, you can do so by adding the following 3 options:

*  `--id': Stop the service with the specified `id'.
*  `--host`: Stop all services on the specified host.
*  `--role`: stops all services for the specified role

The above 3 options can be used in any combination, and the corresponding `id`, `host`, and `role` of the service can be viewed via [dingoadm status](#Client_Status).

#### Example 1: Stopping a service with id `c9570c0d0252`

```shell
$ dingoadm stop --id c9570c0d0252
```

#### Example 2: Stopping All `MDS` Services on the `10.0.1.1` Host
```shell
$ dingoadm stop --host 10.0.1.1 --role mds
```

> ⚠️ **Warning:**
>
> Stopping the service may cause an unhealthy cluster and result in client IO failures, so please proceed with caution.

## Restart services
<a id="Restart"></a>

```shell
$ dingoadm restart
```

DingoAdm restarts all services in the cluster by default. If you want to restart a specific service, you can do so by adding the following 3 options:

*  `--id': restarts the service with the specified `id'.
*  `--host`: Restarts all services on the specified host.
*  `--role`: restarts all services for the specified role

The above 3 options can be used in any combination, and the corresponding `id`, `host`, and `role` of the service can be viewed via [dingoadm status](#Client_Status).

#### Example 1: Restarting a service with id `c9570c0d0252`

```shell
$ dingoadm restart --id c9570c0d0252
```

#### Example 2: Restarting All `MDS` Services on the `10.0.1.1` Host
```shell
$ dingoadm restart --host 10.0.1.1 --role mds
```

## Modifying the Service Configuration

During cluster operation, we may need to modify the configuration of the service and restart the service. The steps are as follows:

#### Step 1: Edit the local cluster topology file and modify the configuration entries for the corresponding services

```shell
$ vim topology.yaml
```

> 💡 **Reminder:**
>
> When the local topology file is lost, we can recover it by saving the current cluster topology:
> ```shell
> $ dingoadm config show > topology.yaml
> ```

#### Step 2: Submit changes
```shell
$ dingoadm config commit topology.yaml
```

#### Step 3: Reload Services

```shell
$ dingoadm reload
```

DingoAdm reloads all services in the cluster by default, if you want to reload a specific service, you can do so by adding the following 3 options:

*  `--id`: Reloads the service with the specified `id`.
*  `--host`: Reloads all services on the specified host.
*  `--role`: Reloads all services for the specified role

The above 3 options can be used in any combination, and the corresponding `id`, `host`, and `role` of the service can be viewed via [dingoadm status](#Client_Status).

#### Example 1: Reloading the service with id `c9570c0d0252`

```shell
$ dingoadm reload --id c9570c0d0252
```

#### Example 2: Reloading all `MDS` services on the `10.0.1.1` host
```shell
$ dingoadm reload --host 10.0.1.1 --role mds
```

> 💡 **Reminder:**
>
> The difference between the command [restart](#Restart) and `reload` is that the
> `reload` modifies the configuration of the corresponding service based on the current cluster topology changes, and then restarts the service.
> `restart` simply restarts the service.

## Entering the Service Container

We can remotely go inside the service container and view information about the service processes, configuration, logs, data, etc.:.

```shell
$ dingoadm enter < id>
```

The `id` corresponding to a service can be viewed via [dingoadm status](#Client_Status).

> 💡 **Reminder:**
>
> DingoAdm defaults to the root directory of the service. The service root directory contains all the files required by the service and has the following directory structure:
>
> ```shell
> /dingofs/mds  # Service root directory
>| -- conf      # Configuration directory
>|   ` -- mds.conf
>| -- data      # Data catalogue
>| -- logs      # Log directory
>|   ` -- dingofs-mds.log.INFO.20220120-142115.6
>` -- sbin      # Binary directory
>    ` -- dingofs-mds
> ```

## Clean up the cluster

```shell
$ dingoadm clean
```

DingoAdm cleans up all objects of all services in the cluster by default. If you want to clean up a specific service or object, you can do so by adding the following 4 options:

*  `--id`: Cleans up the service corresponding to the specified `id`.
*  `--host`: Cleans up the services corresponding to the specified host.
*  `--role`: cleans up the services corresponding to the specified role
*  `--only`: clean up the specified objects (`log`: log, `data`: data, `container`: container)

The above 4 options can be used in any combination, and the corresponding `id`, `host`, and `role` of the service can be viewed via [dingoadm status](#Client_Status).

#### Example 1: Cleaning up all objects with id `c9570c0d0252` service

```shell
$ dingoadm clean --id c9570c0d0252
```

#### Example 2: Cleaning up logs and data from all MDS services

```shell
$ dingoadm clean --role mds --only log,data
```

#### Example 3: Cleaning up all objects of the `MDS` service on the `10.0.1.1` host

```shell
$ dingoadm clean --host 10.0.1.1 --role mds
```

> 📢 **Note:**
>
> When cleaning up a service container, make sure that the corresponding service is stopped; you can use the [stop](#Discontinuation) command to stop the specified service.

## Viewing Client Status
<a id="Client_Status"></a>

```shell
$ dingoadm client status
```

DingoAdm displays the Client ID, Client Type, Host, Container ID, Running Status, and Auxiliary information by default:

```shell
Get Client Status: [OK]

Id            Kind     Host          Container Id  Status       Aux Info
--            ----     ----          ------------  ------       --------
362d538778ad  dingofs  server-host1  cfa00fd01ae8  Up 36 hours  {"user":"dingofs","volume":"/test1"}
b0d56cfaad14  dingofs  server-host2  c0301eff2af0  Up 36 hours  {"user":"dingofs","volume":"/test2"}
c700e1f6acab  dingofs  server-host3  52554173a54f  Up 36 hours  {"user":"dingofs","volume":"/test3"}
```

## Entering the Client Container

We can remotely go inside the service container and view information about client processes, configuration, logs, data, etc:.

```shell
$ dingoadm client < id>
```

The corresponding `id` of the client can be viewed via [dingoadm client status](#Client_Status).

> 💡 **Reminder:**
>
> DingoAdm defaults to the root directory of this client. The client root directory contains all the files required by the service and has the following directory structure:
>
> ```shell
> /dingofs/client # Service root directory
>| -- conf      # Configuration directory
>|   ` -- client.conf
>| -- data      # Data catalogue
>| -- logs      # Log directory
>|   ` -- dingofs-client.log.INFO.20220120-142115.6
>` -- sbin      # Binary directory
>    ` -- dingo-client
> ```
