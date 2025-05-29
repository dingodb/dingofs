#!/usr/bin/env bash

# $1: os

g_image_base="dingodatabase/dingofs-base"
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
# tmpl.sh = /usr/local/metaserver.conf /tmp/metaserver.conf
function tmpl() {
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

############################  MAIN()
docker_prefix="$(pwd)/docker/$1"
prefix="$docker_prefix/dingofs" # /path/to/dingofs/docker/rocky9/dingofs
mkdir -p $prefix $prefix/conf
install_pkg $prefix
install_pkg $prefix etcd
install_pkg $prefix monitor

paths=`ls conf/*`
# paths="$paths tools-v2/pkg/config/dingo.yaml"
for path in $paths;
do
    dir=`dirname $path`
    file=`basename $path`

    # delimiter
    dsv="="
    if [ $file = "etcd.conf" -o $file = "dingo.yaml" ]; then
        dsv=": "
    fi

    # destination
    dst=$file
    if [ $file = "snapshot_clone_server.conf" ]; then
        dst="snapshotclone.conf"
    fi

    tmpl $dsv "$dir/$file" "$prefix/conf/$dst"
    success "install $file success\n"
done

cp thirdparties/etcdclient/libetcdclient.so $prefix/etcd/lib/

# Add build-image parameter check with default value 'false'
build_image=${3:-0}
g_docker="${DINGO_DOCKER:=docker}"

case "$build_image" in
    1)
        ${g_docker} pull $g_image_base:$1
        ${g_docker} build --no-cache -t "$2" "$docker_prefix"
        success "build $2 success\n"
        ;;
    2)
        ${g_docker} pull $g_image_base:$1
        ${g_docker} build --no-cache -t "$2" -f "$docker_prefix/Dockerfile-v2" "$docker_prefix"
        success "build $2 success\n"
        ;;
    *)
        msg "skip build image\n"
        ;;
esac
