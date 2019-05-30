#!/usr/bin/env bash

scp -r /home/john/AutoDP john@10.22.1.1:/home/john/ & #1
scp -r /home/john/AutoDP john@10.22.1.2:/home/john/ & #2
#scp -r /home/john/tpch-spark john@10.22.1.3:/home/john/ & #3
scp -r /home/john/AutoDP john@10.22.1.4:/home/john/ & #4
scp -r /home/john/AutoDP john@10.22.1.5:/home/john/ &#5
scp -r /home/john/AutoDP john@10.22.1.6:/home/john/ & #6
scp -r /home/john/AutoDP john@10.22.1.7:/home/john/ & #7
scp -r /home/john/AutoDP john@10.22.1.8:/home/john/ & #8
scp -r /home/john/AutoDP john@10.22.1.9:/home/john/ & #9
scp -r /home/john/AutoDP john@10.22.1.10:/home/john/  #10

ssh john@10.22.1.1 "cd /home/john/AutoDP; build/mvn -DskipTests -T 40 clean package" &
ssh john@10.22.1.2 "cd /home/john/AutoDP; build/mvn -DskipTests -T 40 clean package" &
ssh john@10.22.1.4 "cd /home/john/AutoDP; build/mvn -DskipTests -T 40 clean package" &
ssh john@10.22.1.5 "cd /home/john/AutoDP; build/mvn -DskipTests -T 40 clean package" &
ssh john@10.22.1.6 "cd /home/john/AutoDP; build/mvn -DskipTests -T 40 clean package" &
ssh john@10.22.1.7 "cd /home/john/AutoDP; build/mvn -DskipTests -T 40 clean package" &
ssh john@10.22.1.8 "cd /home/john/AutoDP; build/mvn -DskipTests -T 40 clean package" &
ssh john@10.22.1.9 "cd /home/john/AutoDP; build/mvn -DskipTests -T 40 clean package" &
ssh john@10.22.1.10 "cd /home/john/AutoDP; build/mvn -DskipTests -T 40 clean package"

#scp -r /home/john/tpch-spark john@10.22.1.6:/home/john/ & #6
#scp -r /home/john/tpch-spark john@10.22.1.7:/home/john/ & #7
#scp -r /home/john/tpch-spark john@10.22.1.8:/home/john/ & #8
#scp -r /home/john/tpch-spark john@10.22.1.9:/home/john/ & #9
#scp -r /home/john/tpch-spark john@10.22.1.10:/home/john/  #10