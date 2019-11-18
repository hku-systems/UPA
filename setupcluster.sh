#!/bin/bash

c=7081

./sbin/start-master.sh -h 10.22.1.3 -p $c

#ssh john@10.22.1.1 "./AutoDP/sbin/start-slave.sh spark://10.22.1.3:$c"
#ssh john@10.22.1.2 "./AutoDP/sbin/start-slave.sh spark://10.22.1.3:$c"
#ssh john@10.22.1.3 "./AutoDP/sbin/start-slave.sh spark://10.22.1.3:$c"
ssh john@10.22.1.4 "./AutoDP/sbin/start-slave.sh spark://10.22.1.3:7081"
#ssh john@10.22.1.5 "./AutoDP/sbin/start-slave.sh spark://10.22.1.3:$c"
##ssh john@10.22.1.6 "./AutoDP/sbin/start-slave.sh spark://10.22.1.3:$c"
##ssh john@10.22.1.7 "./AutoDP/sbin/start-slave.sh spark://10.22.1.3:$c"
##ssh john@10.22.1.8 "./AutoDP/sbin/start-slave.sh spark://10.22.1.3:$c"
##ssh john@10.22.1.9 "./AutoDP/sbin/start-slave.sh spark://10.22.1.3:$c"
##ssh john@10.22.1.10 "./AutoDP/sbin/start-slave.sh spark://10.22.1.3:$c"

