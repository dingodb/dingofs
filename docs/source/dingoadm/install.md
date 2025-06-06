Software and Hardware Environment Requirements
===
Linux Operating System Version Requirements
---

| Distribution  | Version Requirements      |
| :---   | :---        |
| Debian | Version 9 or higher  |
| CentOS | Version 7 or higher  |
| Ubuntu | Version 20 or higher |

Network Requirements
---

Currently, deploying a cluster via DingoAdm has the following two network requirements:

* The central control machine where DingoAdm is installed must be able to connect to the deployment server via SSH
* Each server must pull images from the Docker repository

Installing Dependencies
===

All DingoFS services run in Docker containers. Users need to install Docker on each server and ensure that the Docker Daemon is running.

You can run the following command on the server to check:

```shell
$ sudo docker run --rm hello-world
```

This command downloads a test image and runs it in a container. When the container is running, it prints a message and exits.


Installing DingoAdm
===

The default installation path is the `.dingoadm` directory under the current user's home directory, i.e., `~/.dingoadm`.

💡 **Reminder:**

DingoAdm includes command completion functionality. Execute the following command and follow the prompts to enable completion:
> ```shell
> $ dingoadm completion -h
> ```

[install-docker]: https://yeasy.gitbook.io/docker_practice/install

DingoAdm Configuration File
===

The DingoAdm configuration file is located in the installation directory as `dingoadm.cfg`, i.e., `~/.dingoadm/dingoadm.cfg`. The configuration options in the DingoAdm configuration file apply to the execution of all DingoAdm commands.

### Example

```ini
[defaults]
log_level = info
sudo_alias = “sudo”
timeout = 300
auto_upgrade = true

[ssh_connections]
retries = 3
timeout = 10
```

### Configuration Item Description

| Block            | Configuration Item       | Default Value | Description                                                                                                                                                                                   |
| :---            | :---         | :---   | :---                                                                                                                                                                                   |
| defaults        | log_level    | info   | Log level. Supports four levels: `debug`, `info`, `warn`, and `error`.                                                                                                                               |
| defaults        | sudo_alias   | sudo   | Sudo alias. DingoAdm requires root privileges to execute certain commands and defaults to executing them using sudo. If users wish to control this behavior, they can modify this configuration item, and commands requiring sudo execution will be replaced with `sudo_alias` |
| defaults        | timeout      | 300    | Command execution timeout (in seconds) |
| ssh_connections | retries      | 3      | Number of retries for failed SSH connections |
| ssh_connections | timeout      | 10     | SSH connection timeout (in seconds)                                                                                                                                                           |
