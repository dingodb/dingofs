# Copyright (C) 2021 Jingli Chen (Wine93), NetEase Inc.

.PHONY: list build dep install image playground check test install_and_config

stor?=""
prefix?= "$(PWD)/projects"
release?= 0
dep?= 0
only?= "*"
tag?= "dingodatabase/dingofs:unknown"
case?= "*"
os?= "rocky9"
ci?=0

define help_msg
## list
Usage:
    make list

## build
Usage:
    make build only=TARGET dep=0/1 release=0/1 os=OS
Examples:
    make build only=test/* os=rocky9
    make build release=1


## dep
Usage:
    make dep

## install
Usage:
    make install prefix=PREFIX only=TARGET
Examples:
    make install prefix=/usr/local/dingofs only=etcd


## image
Usage:
    make image tag=TAG os=OS
Examples:
    make image tag=dingodb/dingofs:v1.2 os=rocky9
endef
export help_msg

help:
	@echo "$$help_msg"

list:
	@bash build-scripts/build.sh --stor=fs --list

file_build:
	@bash build-scripts/file-build.sh --only=$(only) --dep=$(dep) --release=$(release) --os=$(os) --unit_tests=${unit_tests}

file_dep:
	@bash build-scripts/file-build.sh --only="" --dep=1

file_install:
	@bash build-scripts/file-install.sh --prefix=$(prefix) --only=$(only)

file_deploy_config:
	@bash build-scripts/file-deploy-config.sh $(os)

file_image:
	@bash build-scripts/file-deploy-config.sh $(os) $(tag) ${build-image}

playground:
	@bash build-scripts/playground.sh

check:
	@bash build-scripts/check.sh fs

test:
	@bash build-scripts/test.sh fs $(only)
