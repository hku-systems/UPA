#!/bin/bash

./sbin/start-master.sh -h 202.45.128.165 -p 7081

c=7081

#ssh john@10.22.1.1 "./AutoDP/sbin/start-slave.sh spark://10.22.1.3:$c"
#ssh john@10.22.1.2 "./AutoDP/sbin/start-slave.sh spark://10.22.1.3:$c"
#ssh john@10.22.1.3 "./AutoDP/sbin/start-slave.sh spark://10.22.1.3:$c"
#ssh john@10.22.1.4 "./AutoDP/sbin/start-slave.sh spark://10.22.1.3:$c"
#ssh john@10.22.1.5 "./AutoDP/sbin/start-slave.sh spark://10.22.1.3:$c"
#ssh john@10.22.1.6 "./AutoDP/sbin/start-slave.sh spark://10.22.1.3:$c"
ssh john@202.45.128.166 "./AutoDP/sbin/start-slave.sh spark://202.45.128.165:$c"
ssh john@202.45.128.167 "./AutoDP/sbin/start-slave.sh spark://202.45.128.165:$c"
ssh john@202.45.128.168 "./AutoDP/sbin/start-slave.sh spark://202.45.128.165:$c"
#ssh john@10.22.1.10 "./AutoDP/sbin/start-slave.sh spark://10.22.1.6:$c"

