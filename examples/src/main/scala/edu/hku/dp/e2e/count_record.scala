package edu.hku.dp.e2e

import edu.hku.cs.dp.dpread_c
import org.apache.spark.sql.SparkSession

object count_record {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("count_record")
      .getOrCreate()

    println("Running count_record")
    val filter_num = args(2).toInt
    val final_result = new dpread_c(spark.sparkContext.textFile(args(0)))
      .mapDP(_.toDouble,args(1).toInt)
      .filterDP(p => p <= 1000 - filter_num)
      .mapDP(p => 1.0)
      .reduceDP(_+_)

    println("final output: " + final_result)
  }
}
