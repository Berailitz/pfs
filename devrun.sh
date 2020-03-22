#!/usr/bin/env bash

umount x
dlv debug --headless --listen=:18180 --api-version=2
