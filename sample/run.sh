#!/usr/bin/env bash
set -x -e

runCfg="$1"
logDir="./log"
oldLogDir="./log.old"

currentOldLogBaseDir="$oldLogDir/$(date +'%m%d%H')"
currentOldLogDir="$currentOldLogBaseDir/$(date +'%H%m%S')"

[ -z "$runCfg" ] && echo "empty cfg path, exiting" && exit 1

echo "====BUILD START===="
go build -ldflags="-X 'main.gitCommit=\"$(git rev-list -1 --oneline --date=local HEAD)\"' -X 'main.buildTime=\"$(date '+%Y/%m/%d %H:%M:%S')\"'" 2>&1
echo "====ROTATE LOG====="
[ -d "$logDir" ] && mkdir -p "$currentOldLogBaseDir" && mv "$logDir" "$currentOldLogDir"
mkdir -p "$logDir"
echo "====RUN START===="
./sample -cfg="$runCfg" 2>&1
echo "====RUN END===="
