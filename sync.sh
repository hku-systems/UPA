#!/usr/bin/env bash
uname="/home/john"
mname="/Users/lionon"
p_name=$mname
rsync -a -e "ssh -p 2212" --exclude=cmake-build-debug $p_name/Documents/UPA/examples/src/main/scala/edu john@127.0.0.1:/home/john/UPA/examples/src/main/scala/
rsync -a -e "ssh -p 2212" --exclude=cmake-build-debug $p_name/Documents/UPA/core/src/main/scala/edu/hku/cs/dp john@127.0.0.1:/home/john/UPA/core/src/main/scala/edu/hku/cs/
rsync -a -e "ssh -p 2212" --exclude=cmake-build-debug $p_name/Documents/UPA/*.sh john@127.0.0.1:/home/john/UPA/
rsync -a -e "ssh -p 2212" --exclude=cmake-build-debug $p_name/Documents/UPA/*.py john@127.0.0.1:/home/john/UPA/

