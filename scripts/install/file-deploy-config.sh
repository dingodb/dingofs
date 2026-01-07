#!/usr/bin/env bash

# $1: os
g_os="rocky9"
g_image_base="dingodatabase/dingofs-base"
g_image_name="dingodatabase/dingofs:latest"
g_image_type="0" # 0: skip build image, 1: build mds v1 image, 2: build mdsv2 image, 
# g_image_base="harbor.zetyun.cn/dingofs/dingofs-base"

############################  BASIC FUNCTIONS
msg() {
    printf '%b' "$1" >&2
}

success() {
    msg "\33[32m[✔]\33[0m ${1}${2}"
}

die() {
    msg "\33[31m[✘]\33[0m ${1}${2}"
    exit 1
}

############################ FUNCTIONS
# usage: process_config_template  conf/client.conf /path/to/dingofs/scripts/docker/rocky9/dingofs/confg/client.conf
function usage() {
    echo "Usage: $0 [options]"
    echo "Options:"
    echo "  --name <image_name>         Set the name of the Docker image to build"
    echo "  --os <os_name>              Set the operating system (default: rocky9)"
    echo "  --image-type <type>         Set the type of image to build (1 for MDS v1, 2 for MDS v2, 0 to skip)"
    echo "  -h, --help                  Show this help message"
}

function process_config_template() {
    dsv=$1
    src=$2
    dst=$3
    
    regex="^([^$dsv]+$dsv[[:space:]]*)(.+)__DINGOADM_TEMPLATE__[[:space:]]+(.+)[[:space:]]+__DINGOADM_TEMPLATE__(.*)$"
    while IFS= read -r line || [ -n "$line" ]; do
        if [[ ! $line =~ $regex ]]; then
            echo "$line"
        else
            echo "${BASH_REMATCH[1]}${BASH_REMATCH[3]}"
        fi
    done < $src > $dst

}

install_pkg() {
    if [ $# -eq 1 ]; then
        pkg="dingofs"
        make file_install prefix=$1
    else
        pkg=$2
        make file_install prefix=$1 only=$2
    fi

    if [ $? -eq 0 ]; then
        success "install $pkg success\n"
    else
        die "install $pkg failed\n"
    fi
}

get_options() {
    local args=`getopt --long name:,os:,type:,help -n "$0" -- "$@"`
    eval set -- "${args}"
    while true
    do
        case "$1" in
            --name)
                g_image_name=$2
                shift 2
                ;;
            --os)
                g_os=$2
                shift 2
                ;;
            --type)
                g_image_type=$2
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

get_options "$@"

############################  MAIN()
docker_prefix="$(pwd)/scripts/docker/$g_os"
prefix="$docker_prefix/dingofs" # /path/to/dingofs/scripts/docker/rocky9/dingofs
mkdir -p conf $prefix $prefix/conf
install_pkg $prefix
install_pkg $prefix monitor

cp scripts/dev-mds/mds.template.conf $prefix/conf/mds.template.conf
paths=`ls conf/*`
# paths="$paths scripts/dev-mds/mds.template.conf"
for path in $paths;
do
    dir=`dirname $path`
    file=`basename $path`

    # delimiter
    dsv="="
    if [ $file = "dingo.yaml" ]; then
        dsv=": "
    fi

    # destination
    dst=$file
    if [ $file = "snapshot_clone_server.conf" ]; then
        dst="snapshotclone.conf"
    fi

    process_config_template $dsv "$dir/$file" "$prefix/$dir/$dst"
    success "install $file success\n"
done

# Add build-image parameter check with default value 'false'
# build_image=${3:-0}
g_docker="${DINGO_DOCKER:=docker}"

case "$g_image_type" in
    1)
        ${g_docker} pull $g_image_base:$1
        ${g_docker} build --no-cache -t "$g_image_name" "$docker_prefix"
        if [ $? -ne 0 ]; then
            die "build $g_image_name failed\n"
        else
            success "build $g_image_name success\n"
        fi
        ;;
    2)
        ${g_docker} pull $g_image_base:$1
        ${g_docker} build --no-cache -t "$g_image_name" -f "$docker_prefix/Dockerfile" "$docker_prefix"
        if [ $? -ne 0 ]; then
            die "build $g_image_name failed\n"
        else
            success "build $g_image_name success\n"
        fi
        ;;
    *)
        msg "skip build image\n"
        ;;
esac
