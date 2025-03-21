# Host Management

The Host Module is designed to centrally manage user hosts, eliminating the need for users to repeatedly configure SSH connection details across multiple configuration files.

## Host Configuration
### Example Configuration
   ```toml
global:
user: dingo.
ssh_port: 22
private_key_file:/home/dingo/.ssh/id_rsa
hosts:
-host: server-hostl
hostname: 10.0.1.1
-host: server-host2
hostname: 10.0.1.2
-host: server-host3
hostname: 10.0.1.3
-host: client-host
hostname: 10.0.1.4
forward_agent: true
become_user: admin
labels:
-client
```
### Import Host List
| Configuration Item   | Yes/No	| Default Value	  |Description|
|----------|---------------|---------------|---------------|
| host  | Y| |Name of the host |
| hostname  | Y| | IP address/Domain of the host |
| user  | |$USER | SSH user for connecting to the remote host. If become_user is not specified, this user will execute deployment operations. Ensure this user has sudo privileges, as it will be used for mounting/unmounting file systems, managing Docker CLI, etc. |
| ssh_port  | |22 | Port of the remote host's SSH service. |
| private_key_file  | |/home/$USER/.ssh/id_rsa | Path to the private key for SSH authentication. |
| forward_agent  | | false| Whether to use SSH agent forwarding for login. |
| become_user  | | | User for executing deployment operations. If not specified, the user field is used. Ensure this user has sudo privileges. |
| labels  | | |Labels assigned to the host. Multiple tags can be specified  |


## Import Host List
### Prepare Host List
   ```toml
$ vim hosts.yaml
global:
user: dingo.
ssh_port: 22
private_key_file: /home/dingo/.ssh/id_rsa
hosts:
-host: server-host1
hostname: 10.0.1.1
-host: server-host2
hostname: 10.0.1.2
-host: server-host3
hostname: 10.0.1.3
-host: client-host
hostname: 10.0.1.4
forward_agent: true
become_user: admin
labels: - client
   ```
   
### Import Host List
   ```toml
$ dingoadm hosts commit hosts.yam!
   ```
## View Host Configuration
   ```toml
$ dingoadm hosts ls
   ```
DingoAdm displays all hosts by default.
| Host|Hostname|User|Port|Private Key File|Forward Agent|Become User |Labels|
|----------|-----------|---------------|------------|---------|---------------|---------------|---------------|
|server-host1| 10.0.1.1| dingo |22|/home/dingo/.ssh/id_rsa|N| -| -|
|server-host2| 10.0.1.2| dingo |22|/home/dingo/.ssh/id_rsa|N| -| -|
|server-host3| 10.0.1.3| dingo |22|/home/dingo/.ssh/id_rsa|N| -| -|
|client-host| 10.0.1.4| dingo |22|/home/dingo/.ssh/id_rsa|Y| admin| client|

To filter hosts by tags, use the - parameter.

## Display Host Configuration
   ```toml
$ dingoadm hosts show
global:
user: dingo
ssh_port: 22
private_key_file: /home/dingo/.ssh/id_rsa
hosts:
-host: server-host1
hostname: 10.0.1.1
-host: server-host2
hostname: 10.0.1.2
-host: server-host3
hostname: 10.0.1.3
-host: client-host
hostname: 10.0.1.4
forward_agent: true
become_user: admin
labels:
-client
   ```
## Log in to a Host
   ```toml
   $ dingoadm ssh <host> 
  ```
Example: Log in to the host server1
   ```toml
   $ dingoadm ssh serverl 
  ```

Note:
If SSH connection issues occur, you can manually simulate DingoAdm's connection process locally based on the host's configuration to troubleshoot. For example:
   ```toml
$ ssh<user>@<hostname>-p<ssh_port>-i<private_key_file>
  ```