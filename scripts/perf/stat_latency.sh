#!/bin/bash

arg_num=$#
file_name="$1"

if [ $arg_num -ne 1 ]; then
    echo "Usage: $0 <file_name>"
    exit 1
fi

awk '
{
    delete t

    while (match($0, /name: "[^"]+" time_us: [0-9]+/)) {
        s = substr($0, RSTART, RLENGTH)

        match(s, /name: "[^"]+"/)
        name = substr(s, RSTART + 7, RLENGTH - 8)

        match(s, /time_us: [0-9]+/)
        time = substr(s, RSTART + 9, RLENGTH - 9)

        t[name] = time

        $0 = substr($0, RSTART + RLENGTH)
    }

    if ("check_quota" in t) {
        print t["check_quota"],
              t["gen_ino"],
              t["prepare"],
              t["store_queue"],
              t["store_pending"],
              t["store_txn"],
              t["store_finish"],
              t["post_handle"]
    }
}' $file_name