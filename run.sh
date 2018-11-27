#!/bin/bash

#Logistic Regression
#Normanl case
./bin/spark-submit --master spark://10.22.1.13:8700 --class org.apache.spark.examples.SparkHdfsLR examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar /home/john/dataset/kmeans/1mb.csv 8
#DP case
./bin/spark-submit --master spark://10.22.1.13:8700 --class org.apache.spark.examples.SparkHdfsLRDP examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar /home/john/dataset/kmeans/1mb.csv 8
#

./bin/spark-submit --master spark://10.22.1.13:8700 --class org.apache.spark.examples.Count examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar 2 100

./bin/spark-submit --master spark://10.22.1.13:8700 --class org.apache.spark.examples.SparkPiDP examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar 10 10

#KMeans
#Normanl case
./bin/spark-submit --master spark://10.22.1.13:8700 --class org.apache.spark.examples.SparkKMeans examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar /home/john/dataset/kmeans/1mb.csv 5 0.1
#DP case
./bin/spark-submit --master spark://10.22.1.13:8700 --class org.apache.spark.examples.SparkKMeansDP examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar /home/john/dataset/kmeans/1mb.csv 5 0.1
