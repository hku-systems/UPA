#!/bin/bash
echo "Start" > output.txt
#opaque1

formateddate=`date | sed "s~ ~-~g" | sed "s~:~-~g"`

echo "opaque 3 - 100GB" >> output.txt

./bin/spark-submit --master spark://10.22.1.3:7081 --driver-memory 60g  --executor-memory 60g --conf spark.executor.extraJavaOptions="-Xms60g" --conf spark.driver.maxResultSize="0" --class main.scala.Q31DP examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar 100

cd ..