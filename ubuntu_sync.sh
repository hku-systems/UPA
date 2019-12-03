#!/usr/bin/env bash
rsync -a -e "ssh -p 2212" --exclude=cmake-build-debug /home/john/Documents/AutoDP/examples/src/main/scala/edu john@127.0.0.1:/home/john/AutoDP/examples/src/main/scala/
rsync -a -e "ssh -p 2212" --exclude=cmake-build-debug /home/john/Documents/AutoDP/core/src/main/scala/edu/hku/cs/dp john@127.0.0.1:/home/john/AutoDP/core/src/main/scala/edu/hku/cs/
rsync -a -e "ssh -p 2212" --exclude=cmake-build-debug /home/john/Documents/AutoDP/*.sh john@127.0.0.1:/home/john/AutoDP/
rsync -a -e "ssh -p 2212" --exclude=cmake-build-debug /home/john/Documents/AutoDP/*.py john@127.0.0.1:/home/john/AutoDP/
rsync -a -e "ssh -p 2212" --exclude=cmake-build-debug /home/john/Documents/AutoDP/hosts.txt john@127.0.0.1:/home/john/AutoDP/
rsync -a -e "ssh -p 2212" --exclude=cmake-build-debug /home/john/Documents/AutoDP/conf john@127.0.0.1:/home/john/AutoDP/
rsync -a -e "ssh -p 2212" --exclude=cmake-build-debug /home/john/Documents/AutoDP/plot/*.py john@127.0.0.1:/home/john/AutoDP/plot/


#rsync -a -e "ssh -p 2206" --exclude=cmake-build-debug /home/john/Documents/AutoDP/examples/src/main/scala/edu john@127.0.0.1:/home/john/AutoDP/examples/src/main/scala/
#rsync -a -e "ssh -p 2206" --exclude=cmake-build-debug /home/john/Documents/AutoDP/core/src/main/scala/edu/hku/cs/dp john@127.0.0.1:/home/john/AutoDP/core/src/main/scala/edu/hku/cs/
#rsync -a -e "ssh -p 2206" --exclude=cmake-build-debug /home/john/Documents/AutoDP/*.sh john@127.0.0.1:/home/john/AutoDP/
#rsync -a -e "ssh -p 2206" --exclude=cmake-build-debug /home/john/Documents/AutoDP/gen_data.py john@127.0.0.1:/home/john/AutoDP/
#
#rsync -a -e "ssh -p 2210" --exclude=cmake-build-debug /home/john/Documents/AutoDP/examples/src/main/scala/edu john@127.0.0.1:/home/john/AutoDP/examples/src/main/scala/
#rsync -a -e "ssh -p 2210" --exclude=cmake-build-debug /home/john/Documents/AutoDP/core/src/main/scala/edu/hku/cs/dp john@127.0.0.1:/home/john/AutoDP/core/src/main/scala/edu/hku/cs/
#rsync -a -e "ssh -p 2210" --exclude=cmake-build-debug /home/john/Documents/AutoDP/*.sh john@127.0.0.1:/home/john/AutoDP/
#rsync -a -e "ssh -p 2210" --exclude=cmake-build-debug /home/john/Documents/AutoDP/gen_data.py john@127.0.0.1:/home/john/AutoDP/