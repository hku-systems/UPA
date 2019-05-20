#!/bin/bash

#ssh john@10.22.1.12 "./AutoDP/sbin/stop-slave.sh spark://10.22.1.3:$c"
ssh john@10.22.1.13 "./AutoDP/sbin/stop-slave.sh spark://10.22.1.3:$c"
ssh john@10.22.1.14 "./AutoDP/sbin/stop-slave.sh spark://10.22.1.3:$c"
ssh john@10.22.1.15 "./AutoDP/sbin/stop-slave.sh spark://10.22.1.3:$c"
ssh john@10.22.1.16 "./AutoDP/sbin/stop-slave.sh spark://10.22.1.3:$c"
ssh john@10.22.1.18 "./AutoDP/sbin/stop-slave.sh spark://10.22.1.3:$c"
ssh john@10.22.1.19 "./AutoDP/sbin/stop-slave.sh spark://10.22.1.3:$c"
#ssh john@10.22.1.20 "./AutoDP/sbin/stop-slave.sh spark://10.22.1.3:$c"
#ssh john@10.22.1.21 "./AutoDP/sbin/stop-slave.sh spark://10.22.1.3:$c"