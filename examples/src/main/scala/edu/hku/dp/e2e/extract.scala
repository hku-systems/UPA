package edu.hku.dp.e2e

import edu.hku.cs.dp.dpread_c
import org.apache.spark.sql.SparkSession

object extract {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("extract")
      .getOrCreate()

    println("Running extract")
    val filter_num = args(2).toInt
    val final_result = new dpread_c(spark.sparkContext.textFile(args(0)))
      .mapDP(_.toDouble,args(1).toInt)
      .filterDP(p => p <= 1000 - filter_num)
      .mapDP(p => {
        val v = p.toDouble
        if(v > 2)
          0.0
        else
          v
      })
      .reduceDP(_+_)

    println("final output: " + final_result)
  }
}
