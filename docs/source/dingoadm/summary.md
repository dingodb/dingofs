DingoAdm Overview
===

DingoAdm is primarily used for rapid deployment and operation and maintenance of [DingoFS](https://github.com/dingodb/dingofs) clusters.

Key Features
===

* Rapid deployment of DingoFS clusters

  Users can deploy the entire cluster with a single click once the cluster topology file is ready.


* Containerized services

  All components run within Docker containers, addressing compatibility issues across different Linux distributions.


* Manage DingoFS clusters

  Supports common management operations, including one-click service upgrades and cluster scaling.


* Manage multiple clusters simultaneously

  Users can manage multiple DingoFS clusters simultaneously and switch between them as needed.


* Precise error localization

  We assign a unique error code to each error that occurs during deployment, and provide detailed explanations and solutions for each error code. Users can look up related error codes online via the [Error Code List][errno] page.


Overall Design
===

Installation Directory
---

To enable multi-user management, DingoAdm is installed by default in the `.dingoadm` directory under the current user's home directory, with all files stored in this directory, including:

| File             | Description                                                                                                        |
| :---             | :---                                                                                                        |
| bin/dingoadm     | Binary file                                                                                                  |
| dingoadm.cfg     | DingoAdm configuration file, configurable for log levels, SSH connection timeout, sudo permissions, etc. |
| CHANGELOG        | Update log for the current version |
| data/dingoadm.db | [SQLite][sqlite] database file, where all persistent data is stored, including cluster name, cluster topology, container IDs for each service, etc. |
| logs             | Log directory, where a log file is generated and stored each time a command is executed |
| temp             | Temporary file directory |

```shell
/home/dingo/.dingoadm
├── bin
│   └── dingoadm
├── CHANGELOG
├── dingoadm.cfg
├── data
│   └── dingoadm.db
├── logs
│   ├── dingoadm-2022-01-17_17-07-57.log
│   └── dingoadm-2022-01-17_17-08-33.log
└── temp
```

Data Storage
---

All data in DingoAdm that needs to be persisted is stored in an SQLite database.
[SQLite][sqlite] is a self-contained, serverless, zero-configuration, transactional SQL database.
A complete SQLite database is a single file.

The database primarily stores the following four types of data:
  * Host information: including host addresses, SSH ports, private key paths, etc.
  * Cluster information: including cluster names, cluster UUIDs, and the topology corresponding to each cluster
  * Service information: service IDs and container IDs corresponding to services
  * Client information: client IDs and container IDs corresponding to clients

> 💡 **Note:**
>
>  Service IDs are generated based on the cluster topology. Given a fixed cluster topology, we generate a unique ID for each service in the topology according to a fixed set of rules, meaning that service IDs are also fixed.
>
> Based on the service ID, the corresponding container ID can be queried in the database. The host module also stores information such as the host and SSH private key path for each service deployment. Using this information, we can manage the containers corresponding to the services.

Service Management
---

To address component runtime dependency issues, all components in DingoFS run within Docker containers. We package all components, dependencies, and necessary tools for each version into a single image and publish it to the Docker public repository.

DingoAdm connects to the remote server via an SSH client to execute deployment or management commands, and uses the Docker client [Docker CLI][docker-cli] to manage service containers.

[dingofs]: https://github.com/dingodb/dingofs
[git]: https://git-scm.com/
[errno]: ../errno.md
[sqlite]: https://www.sqlite.org/index.html
[docker-cli]: https://docs.docker.com/engine/reference/commandline/cli/