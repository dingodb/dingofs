#!/usr/bin/env bash

# Copyright (C) 2021 Jingli Chen (Wine93), NetEase Inc.

############################  GLOBAL VARIABLES
g_role=""
g_args=""
g_prefix=""
g_binary=""
g_start_args=""
#g_preexec="/dingofs/tools-v2/sbin/daemon"

############################  BASIC FUNCTIONS
function msg() {
    printf '%b' "$1" >&2
}

function success() {
    msg "\33[32m[✔]\33[0m ${1}${2}"
}

function die() {
    msg "\33[31m[✘]\33[0m ${1}${2}"
    exit 1
}

############################ FUNCTIONS
function usage () {
    cat << _EOC_
Usage:
    entrypoint.sh --role=ROLE
    entrypoint.sh --role=ROLE --args=ARGS

Examples:
    entrypoint.sh --role=etcd
    entrypoint.sh --role=client --args="-o default_permissions"
_EOC_
}

function get_options() {
    local long_opts="role:,args:,help"
    local args=`getopt -o ra --long $long_opts -n "$0" -- "$@"`
    eval set -- "${args}"
    while true
    do
        case "$1" in
            -r|--role)
                g_role=$2
                shift 2
                ;;
            -a|--args)
                g_args=$2
                shift 2
                ;;
            -h)
                usage
                exit 1
                ;;
            --)
                shift
                break
                ;;
            *)
                exit 1
                ;;
        esac
    done
}

function prepare() {
    g_prefix="/dingofs/$g_role"
    conf_path="$g_prefix/conf/$g_role.conf"

    case $g_role in
        etcd)
            g_binary="$g_prefix/sbin/etcd"
            g_start_args="--config-file $conf_path"
            ;;
        mds)
            g_binary="$g_prefix/sbin/dingo-mds"
            g_start_args="--confPath $conf_path"
            ;;
        metaserver)
            g_binary="$g_prefix/sbin/dingo-metaserver"
            g_start_args="--confPath $conf_path"
            ;;
        client)
            g_binary="$g_prefix/sbin/dingo-fuse"
            g_start_args="--confPath $conf_path"
            ;;
        monitor)
            g_binary="python3"
            g_start_args="target_json.py"
            ;;
        *)
            usage
            exit 1
            ;;
    esac

    if [ "$g_args" != "" ]; then
        g_start_args=$g_args
    fi
}

function create_directory() {
    chmod 700 "$g_prefix/data"
    if [ "$g_role" == "etcd" ]; then
        mkdir -p "$g_prefix/data/wal"
    elif [ "$g_role" == "metaserver" ]; then
        mkdir -p "$g_prefix/data/storage"
    elif [ "$g_role" == "client" ]; then
        mkdir -p "$g_prefix/mnt"
    fi
}

function main() {
    get_options "$@"

    prepare
    create_directory
    [[ $(command -v crontab) ]] && cron
    #[[ ! -z $g_preexec ]] && $g_preexec &
    if [ $g_role == "etcd" ]; then
        exec $g_binary $g_start_args >>$g_prefix/logs/etcd.log 2>&1
    elif [ $g_role == "monitor" ]; then
        cd $g_prefix
        exec $g_binary $g_start_args
    else
        exec $g_binary $g_start_args
    fi

}

############################  MAIN()
main "$@"
