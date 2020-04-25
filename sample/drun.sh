#!/usr/bin/env bash

debugPort=18180
debugCfg='x.yaml'

set -x
umount x

set -x -e
echo "====BUILD START===="
go build -gcflags "all=-N -l" -ldflags="-X 'main.gitCommit=\"$(git rev-list -1 --oneline --date=local HEAD)\"' -X 'main.buildTime=\"$(date '+%Y/%m/%d %H:%M:%S')\"'"
echo "====BUILD END===="

echo "====DEBUG START===="
dlv --listen=:$debugPort --headless=true --api-version=2 --accept-multiclient exec ./sample -- -cfg="$debugCfg"
echo "====DEBUG END===="
