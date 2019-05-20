#!/bin/bash
#3,12-21, no 17

./sbin/start-master.sh -h 10.22.1.3 -p 7081

#logfile=`ls logs | grep "org.apache.spark.deploy.master" | head -1`
#filepath="logs/$logfile"
#b=`cat $filepath | grep "Successfully started service 'sparkMaster' on port"`
#c=${b:${#b}-5:4}

c=7081

#ssh john@10.22.1.12 "./AutoDP/sbin/start-slave.sh spark://10.22.1.3:$c"
ssh john@10.22.1.13 "./AutoDP/sbin/start-slave.sh spark://10.22.1.3:$c"
ssh john@10.22.1.14 "./AutoDP/sbin/start-slave.sh spark://10.22.1.3:$c"
ssh john@10.22.1.15 "./AutoDP/sbin/start-slave.sh spark://10.22.1.3:$c"
ssh john@10.22.1.16 "./AutoDP/sbin/start-slave.sh spark://10.22.1.3:$c"
ssh john@10.22.1.18 "./AutoDP/sbin/start-slave.sh spark://10.22.1.3:$c"
ssh john@10.22.1.19 "./AutoDP/sbin/start-slave.sh spark://10.22.1.3:$c"
#ssh john@10.22.1.20 "./AutoDP/sbin/start-slave.sh spark://10.22.1.3:$c"
#ssh john@10.22.1.21 "./AutoDP/sbin/start-slave.sh spark://10.22.1.3:$c"

#cd AutoDP
#rm output.txt
#echo "Start" > output.txt
##opaque1
#
#formateddate=`date | sed "s~ ~-~g" | sed "s~:~-~g"`
#
#echo "opaque 3 - 100GB" >> output.txt
#
#./bin/spark-submit --master spark://10.22.1.3:$c --driver-memory 60g --executor-cores 1 --executor-memory 60g --conf spark.executor.extraJavaOptions="-Xms60g" --conf spark.driver.maxResultSize="0" --class org.apache.spark.examples.opaque1 examples/target/scala-2.11/jars/spark-examples_2.11-2.1.0.jar
#
#cd ..