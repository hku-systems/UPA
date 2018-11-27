package org.apache.spark.examples

import edu.hku.cs.dp.dpread
import org.apache.spark.sql.SparkSession

import scala.math.random

/**
  * Created by lionon on 11/26/18.
  */
object Count {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("Spark Pi")
      .getOrCreate()
    val slices = if (args.length > 0) args(0).toInt else 2
    val startTime = System.nanoTime()
    val count = new dpread[Int](spark.sparkContext.parallelize(1 until args(1).toInt, slices))
    val countresult = count.mapDP{ i => i }.reduceDP(_ + _)

    //**************Add noise**********************
    val original = countresult._2
    val localSensitivity = countresult._1.map(qq => {
      math.abs(original - qq)
    })
    //********End of add noise**********************

    val elapsedTime = (System.nanoTime() - startTime)
    println("elapsedTime: " + elapsedTime)
    println("countresult: " + countresult._2)
    println("max local sensitivity: " + localSensitivity.max)
  }
}
