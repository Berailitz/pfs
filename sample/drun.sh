#!/usr/bin/env bash

debugPort=18180
runCfg="$1"

set -x
umount x
umount y

set -x -e

[ -z "$runCfg" ] && echo "empty cfg path, exiting" && exit 1

echo "====BUILD START===="
go build -gcflags "all=-N -l" -ldflags="-X 'main.gitCommit=\"$(git rev-list -1 --oneline --date=local HEAD)\"' -X 'main.buildTime=\"$(date '+%Y/%m/%d %H:%M:%S')\"'" 2>&1
echo "====BUILD END===="

echo "====DEBUG START===="
dlv --listen=:$debugPort --headless=true --api-version=2 --accept-multiclient exec ./sample -- -cfg="$runCfg" 2>&1
echo "====DEBUG END===="
