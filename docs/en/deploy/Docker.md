# Docker

DingoFS services run within Docker containers. Users must install Docker on each server and ensure the Docker Daemon is operational.

   ```
$ sudo docker run --rm hello-world
  ```
This command will download a test image and run it in a container. When the container starts, it prints a message and then exits.

## 1. Install Docker Engine on CentOS
To get started with Docker Engine on CentOS, make sure you meet the prerequisites, and then follow the installation steps.
## Installation methods
You can install Docker Engine in different ways, depending on your needs:
- You can set up Docker's repositories and install from them, for ease of installation and upgrade tasks. This is the recommended approach.

- You can download the RPM package, install it manually, and manage upgrades completely manually. This is useful in situations such as installing Docker on air-gapped systems with no access to the internet.

- In testing and development environments, you can use automated convenience scripts to install Docker.

## Setup Guide
### config repo
  ```
sudo dnf config-manager --add-repo https://download.docker.com/linux/centos/docker-ce.repo 
  ```
### install
  ```
sudo dnf -y install docker-ce docker-ce-cli containerd.io docker-compose-plugin
  ```
### start docker service
  ```
sudo systemctl --now enable docker 
  ```

## 2. Install Docker Engine on Ubuntu
To get started with Docker Engine on Ubuntu, make sure you meet the prerequisites, and then follow the installation steps.
### Installation methods
You can install Docker Engine in different ways, depending on your needs:
- Docker Engine comes bundled with Docker Desktop for Linux. This is the easiest and quickest way to get started.

- Set up and install Docker Engine from Docker's apt repository.

- Install it manually and manage upgrades manually.

- Use a convenience script. Only recommended for testing and development environments.

## Setup Guide
### Set up Docker's apt repository.
  ```
# Add Docker's official GPG key:
sudo apt-get update
sudo apt-get install ca-certificates curl
sudo install -m 0755 -d /etc/apt/keyrings
sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
sudo chmod a+r /etc/apt/keyrings/docker.asc

# Add the repository to Apt sources:
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu \
  $(. /etc/os-release && echo "${UBUNTU_CODENAME:-$VERSION_CODENAME}") stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt-get update
  ```
### Install the Docker packages.
  ```
sudo apt-get install docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
  ```
### Verify that the installation is successful by running the hello-world image:
  ```
sudo docker run hello-world
  ```