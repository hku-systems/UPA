#!/usr/bin/env bash

ssh john@10.22.1.12 "rm -rf /home/john/tpch-spark" &
ssh john@10.22.1.13 "rm -rf /home/john/tpch-spark" &
ssh john@10.22.1.14 "rm -rf /home/john/tpch-spark" &
ssh john@10.22.1.15 "rm -rf /home/john/tpch-spark" &
ssh john@10.22.1.16 "rm -rf /home/john/tpch-spark" &
ssh john@10.22.1.18 "rm -rf /home/john/tpch-spark" &
ssh john@10.22.1.19 "rm -rf /home/john/tpch-spark" &
ssh john@10.22.1.20 "rm -rf /home/john/tpch-spark" &
ssh john@10.22.1.21 "rm -rf /home/john/tpch-spark"

scp -r /home/john/tpch-spark john@10.22.1.12:/home/john/ & #1
scp -r /home/john/tpch-spark john@10.22.1.13:/home/john/ & #2
scp -r /home/john/tpch-spark john@10.22.1.14:/home/john/ & #3
scp -r /home/john/tpch-spark john@10.22.1.15:/home/john/ & #4
scp -r /home/john/tpch-spark john@10.22.1.16:/home/john/ & #5
scp -r /home/john/tpch-spark john@10.22.1.17:/home/john/ & #6
scp -r /home/john/tpch-spark john@10.22.1.18:/home/john/ & #7
scp -r /home/john/tpch-spark john@10.22.1.19:/home/john/ & #8
scp -r /home/john/tpch-spark john@10.22.1.20:/home/john/ & #9
scp -r /home/john/tpch-spark john@10.22.1.21:/home/john/  #10