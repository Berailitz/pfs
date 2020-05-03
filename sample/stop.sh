#!/usr/bin/env bash

set -x

debugPort=18180

umount x
umount y

kill "$(ps -ef|grep "dlv --listen=:$debugPort"|head -n 1|awk '{print $2}')"
