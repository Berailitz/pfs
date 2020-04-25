#!/usr/bin/env bash
set -x -e

runCfg="x.yaml"

echo "====BUILD START===="
go build -ldflags="-X 'main.gitCommit=\"$(git rev-list -1 --oneline --date=local HEAD)\"' -X 'main.buildTime=\"$(date '+%Y/%m/%d %H:%M:%S')\"'"
echo "====RUN START===="
./sample -cfg="$runCfg"
echo "====RUN END===="
