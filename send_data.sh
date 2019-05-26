#!/usr/bin/env bash

ssh john@10.22.1.1 "rm -rf /home/john/tpch-spark" &
ssh john@10.22.1.2 "rm -rf /home/john/tpch-spark" &
#ssh john@10.22.1.3 "rm -rf /home/john/tpch-spark" &
ssh john@10.22.1.4 "rm -rf /home/john/tpch-spark" &
ssh john@10.22.1.5 "rm -rf /home/john/tpch-spark"
#ssh john@10.22.1.6 "rm -rf /home/john/tpch-spark" &
#ssh john@10.22.1.7 "rm -rf /home/john/tpch-spark" &
#ssh john@10.22.1.8 "rm -rf /home/john/tpch-spark" &
#ssh john@10.22.1.9 "rm -rf /home/john/tpch-spark" &
#ssh john@10.22.1.10 "rm -rf /home/john/tpch-spark"

scp -r /home/john/tpch-spark john@10.22.1.1:/home/john/ & #1
scp -r /home/john/tpch-spark john@10.22.1.2:/home/john/ & #2
#scp -r /home/john/tpch-spark john@10.22.1.3:/home/john/ & #3
scp -r /home/john/tpch-spark john@10.22.1.4:/home/john/ & #4
scp -r /home/john/tpch-spark john@10.22.1.5:/home/john/  #5
#scp -r /home/john/tpch-spark john@10.22.1.6:/home/john/ & #6
#scp -r /home/john/tpch-spark john@10.22.1.7:/home/john/ & #7
#scp -r /home/john/tpch-spark john@10.22.1.8:/home/john/ & #8
#scp -r /home/john/tpch-spark john@10.22.1.9:/home/john/ & #9
#scp -r /home/john/tpch-spark john@10.22.1.10:/home/john/  #10