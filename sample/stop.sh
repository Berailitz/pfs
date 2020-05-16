#!/usr/bin/env bash

set -x

debugPort=18180

umount -f x
umount -f y

kill "$(ps -ef|grep "dlv --listen=:$debugPort"|head -n 1|awk '{print $2}')"
kill "$(ps -ef|grep "sample"|head -n 1|awk '{print $2}')"
