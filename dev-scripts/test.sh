#!/bin/bash


mydir="${BASH_SOURCE%/*}"
if [[ ! -d "$mydir" ]]; then mydir="$PWD"; fi
. $mydir/shflags

DEFINE_string path './' 'path'
DEFINE_string type 'file' 'file or dir'
DEFINE_integer num 1 'file/dir number'


# parse the command-line
FLAGS "$@" || exit 1
eval set -- "${FLAGS_ARGV}"


for ((i=1; i<=${FLAGS_num}; ++i)); do
  padded=$(printf "%07d" $i)
  
  if [ "${FLAGS_type}" == "file" ]; then
    file_path=${FLAGS_path}/file${padded}
    touch ${file_path}

  elif [ "${FLAGS_type}" == "dir" ]; then
    dir_path=${FLAGS_path}/dir${padded}
    mkdir -p ${dir_path}

  else
    echo "type(${FLAGS_type}) is not supported"
    exit -1
  fi
  
done