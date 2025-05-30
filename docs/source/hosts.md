Host Management
===

Module Overview
---

The host module is used to centrally manage user hosts, reducing the need for users to repeatedly enter host SSH connection configurations across various configuration files.

Host Configuration
---

### Example

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
    forward_agent: true
    become_user: admin
    labels:
      - client
```

### Host configuration items

| Configuration item           | Required | Default value                  | Description                                                                                                                                                                                           |
| :---             | :---     | :---                    | :---                                                                                                                                                                                           |
| host             | Y        |                         | Hostname                                                                                                                                                                                         |
| hostname         | Y        |                         | Host address                                                                                                                                                                                       |
| user             |          | $USER                   | User connecting to the remote host's SSH service. If no user is specified in `become_user`, the user specified in `user` will be used as the execution user for deployment operations. Ensure that the execution user has sudo privileges, as it will be used for operations such as mounting and unmounting file systems and operating the Docker CLI. |
| ssh_port         |          | 22                      | Remote host SSH service port                                                                                                                                                                          |
| private_key_file |          | /home/$USER/.ssh/id_rsa | Path to the private key used to connect to the remote host's SSH service. |
| forward_agent    |          | false                   | Whether to log in to the remote host using SSH agent forwarding. |
| become_user      |          |                         | The user who performs deployment operations. If the user does not specify `become_user`, the user specified by `user` will be used as the execution user for deployment operations. Ensure that the execution user has sudo permissions, as it will be used for operations such as mounting/unmounting file systems and operating the Docker CLI.          |
| labels           |          |                         | Host labels. Multiple labels can be specified for a single host.                                                                                                                                                               |

Manage Hosts
---

* [Import Host List](#Import Host List)
* [View Host List](#View Host List)
* [Display Host Configuration](#Display Host Configuration)
* [Login to Host](#Login to Host)

### Import Host List

#### Step 1: Prepare the host list

```
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
    forward_agent: true
    become_user: admin
    labels:
      - client
```

#### Step 2: Import the host list

```shell
$ dingoadm hosts commit hosts.yaml
```

### View the host list

```shell
$ dingoadm hosts ls
```

DingoAdm will display all hosts by default:

```
Host          Hostname  User   Port  Private Key File         Forward Agent  Become User  Labels
----          --------  ----   ----  ----------------         -------------  -----------  ------
server-host1  10.0.1.1  dingo  22    /home/dingo/.ssh/id_rsa  N              -            -
server-host2  10.0.1.2  dingo  22    /home/dingo/.ssh/id_rsa  N              -            -
server-host3  10.0.1.3  dingo  22    /home/dingo/.ssh/id_rsa  N              -            -
client-host   10.0.1.4  dingo  22    /home/dingo/.ssh/id_rsa  Y              admin        client
```

* To view the list of hosts with a specific tag, specify the `-l` parameter.

### Display host configuration

```shell
$ dingoadm hosts show
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
    forward_agent: true
    become_user: admin
    labels:
      - client
```

### Logging in to the host

```shell
$ dingoadm ssh <host>
```

#### Example: Logging in to the `server1` host

```
$ dingoadm ssh server1
```

> 💡 **Reminder:**
>
> When encountering SSH connection issues, you can manually simulate dingoadm's connection operations locally based on the host's configuration
> to troubleshoot the corresponding issues:
> ```shell
> $ ssh <user>@<hostname> -p <ssh_port> -i <private_key_file>
> ```
