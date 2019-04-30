#!/usr/bin/env bash
rsync -a -e "ssh -p 2201" --exclude=cmake-build-debug /Users/lionon/Documents/AutoDP john@127.0.0.1:/home/john/