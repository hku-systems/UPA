package edu.hku.dp.e2e

import edu.hku.cs.dp.dpread_c
import org.apache.spark.sql.SparkSession

object simpleTest {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("simpleTest")
      .getOrCreate()

    val t1 = System.nanoTime

    val final_result = new dpread_c(spark.sparkContext.textFile(args(0)))
      .mapDP(_.toDouble,args(1).toInt)
      .reduceDP(_+_)

    val duration = (System.nanoTime - t1) / 1e9d
    println("final output: " + final_result)
//    println("noise: " + final_result._2)
//    println("error: " + final_result._2/final_result._1)
//    println("min bound: " + final_result._3)
//    println("max bound: " + final_result._4)
//    println("Execution time: " + duration)
  }
}
