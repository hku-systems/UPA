#!/bin/bash
./bin/spark-submit --master spark://10.22.1.13:8700 --class org.apache.spark.examples.SparkKMeansDP examples/target/scala-2.11/jars/spark-examples_2.11-2.2.0.jar /home/john/dataset/kmeans/1kb.csv 5 0.1
