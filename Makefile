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
    make install prefix=/usr/local/curvefs only=etcd


## image
Usage:
    make image tag=TAG os=OS
Examples:
    make image tag=dingodb/curvefs:v1.2 os=rocky9
endef
export help_msg

help:
	@echo "$$help_msg"

list:
	@bash build-scripts/build.sh --stor=fs --list

build:
	@bash build-scripts/build.sh --stor=fs --only=$(only) --dep=$(dep) --release=$(release) --ci=$(ci) --os=$(os)

dep:
	@bash build-scripts/build.sh --stor=fs --only="" --dep=1

install:
	@bash build-scripts/install.sh --stor=fs --prefix=$(prefix) --only=$(only)

install_and_config:
	@bash build-scripts/install_and_config.sh fs $(os)

image:
	@bash build-scripts/image.sh fs $(tag) $(os)

playground:
	@bash build-scripts/playground.sh

check:
	@bash build-scripts/check.sh fs

test:
	@bash build-scripts/test.sh fs $(only)
