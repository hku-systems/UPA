package edu.hku.dp.e2e

import edu.hku.cs.dp.dpread_c
import org.apache.spark.sql.SparkSession

object sum_all {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("sum_all")
      .getOrCreate()

    println("Running sum_all")

    val filter_num = args(2).toInt
    val final_result = new dpread_c(spark.sparkContext.textFile(args(0)))
      .mapDP(_.toDouble,args(1).toInt)
      .filterDP(p => p <= 1000 - filter_num)
      .reduceDP(_+_)

    println("final output: " + final_result)
  }
}
