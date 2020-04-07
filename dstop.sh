#!/usr/bin/env bash

set -x

dir="x"
debugPort=18180

umount $dir
kill "$(ps -ef|grep "dlv --listen=:$debugPort"|head -n 1|awk '{print $2}')"
