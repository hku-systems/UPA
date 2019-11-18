#!/bin/bash

./sbin/stop-master.sh
#ssh john@10.22.1.1 "./AutoDP/sbin/stop-slave.sh spark://10.22.1.3:$c"
#ssh john@10.22.1.2 "./AutoDP/sbin/stop-slave.sh spark://10.22.1.3:$c"
#ssh john@10.22.1.3 "./AutoDP/sbin/stop-slave.sh spark://10.22.1.3:$c"
ssh john@10.22.1.4 "./AutoDP/sbin/stop-slave.sh spark://10.22.1.3:7081"
ssh john@10.22.1.6 "./AutoDP/sbin/stop-slave.sh spark://10.22.1.3:7081"
#ssh john@10.22.1.5 "./AutoDP/sbin/stop-slave.sh spark://10.22.1.3:$c"
#ssh john@10.22.1.6 "./AutoDP/sbin/stop-slave.sh spark://10.22.1.3:$c"
#ssh john@10.22.1.7 "./AutoDP/sbin/stop-slave.sh spark://10.22.1.3:$c"
#ssh john@10.22.1.8 "./AutoDP/sbin/stop-slave.sh spark://10.22.1.3:$c"
#ssh john@10.22.1.9 "./AutoDP/sbin/stop-slave.sh spark://10.22.1.3:$c"
#ssh john@10.22.1.10 "./AutoDP/sbin/stop-slave.sh spark://10.22.1.3:$c"
