#!/usr/bin/env bash
#rsync -a -e "ssh -p 2203" --exclude=cmake-build-debug /Users/lionon/Documents/AutoDP/examples/src/main/scala/edu john@127.0.0.1:/home/john/AutoDP/examples/src/main/scala/
#rsync -a -e "ssh -p 2203" --exclude=cmake-build-debug /Users/lionon/Documents/AutoDP/core/src/main/scala/edu/hku/cs/dp john@127.0.0.1:/home/john/AutoDP/core/src/main/scala/edu/hku/cs/
#rsync -a -e "ssh -p 2203" --exclude=cmake-build-debug /Users/lionon/Documents/AutoDP/*.sh john@127.0.0.1:/home/john/AutoDP/
#rsync -a -e "ssh -p 2203" --exclude=cmake-build-debug /Users/lionon/Documents/AutoDP/gen_data.py john@127.0.0.1:/home/john/AutoDP/

rsync -a -e "ssh -p 2206" --exclude=cmake-build-debug /Users/lionon/Documents/AutoDP/examples/src/main/scala/edu john@127.0.0.1:/home/john/AutoDP/examples/src/main/scala/
rsync -a -e "ssh -p 2206" --exclude=cmake-build-debug /Users/lionon/Documents/AutoDP/core/src/main/scala/edu/hku/cs/dp john@127.0.0.1:/home/john/AutoDP/core/src/main/scala/edu/hku/cs/
rsync -a -e "ssh -p 2206" --exclude=cmake-build-debug /Users/lionon/Documents/AutoDP/*.sh john@127.0.0.1:/home/john/AutoDP/
rsync -a -e "ssh -p 2206" --exclude=cmake-build-debug /Users/lionon/Documents/AutoDP/gen_data.py john@127.0.0.1:/home/john/AutoDP/

rsync -a -e "ssh -p 2210" --exclude=cmake-build-debug /Users/lionon/Documents/AutoDP/examples/src/main/scala/edu john@127.0.0.1:/home/john/AutoDP/examples/src/main/scala/
rsync -a -e "ssh -p 2210" --exclude=cmake-build-debug /Users/lionon/Documents/AutoDP/core/src/main/scala/edu/hku/cs/dp john@127.0.0.1:/home/john/AutoDP/core/src/main/scala/edu/hku/cs/
rsync -a -e "ssh -p 2210" --exclude=cmake-build-debug /Users/lionon/Documents/AutoDP/*.sh john@127.0.0.1:/home/john/AutoDP/
rsync -a -e "ssh -p 2210" --exclude=cmake-build-debug /Users/lionon/Documents/AutoDP/gen_data.py john@127.0.0.1:/home/john/AutoDP/
