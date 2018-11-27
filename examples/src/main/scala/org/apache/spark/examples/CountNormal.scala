package org.apache.spark.examples

import org.apache.spark.sql.SparkSession

/**
  * Created by lionon on 11/26/18.
  */
object CountNormal {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("Spark Pi")
      .getOrCreate()
    val slices = if (args.length > 0) args(0).toInt else 2
    val startTime = System.nanoTime()
    val count = spark.sparkContext.parallelize(1 until args(1).toInt, slices)
    val countresult = count.map{ i => i }.reduce(_ + _)
    val elapsedTime = (System.nanoTime() - startTime)
    println("elapsedTime: " + elapsedTime)
    println("countresult: " + countresult)
  }
}
