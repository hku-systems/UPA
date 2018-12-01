package org.apache.spark.examples

import edu.hku.cs.dp.dpread
import org.apache.spark.sql.SparkSession

/**
  * Created by lionon on 12/1/18.
  */
object SVMDPNormal {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("SVMDPNormal")
      .getOrCreate()

    val count = spark.read.textFile(args(0)).rdd

    val countresult = count.map(line => {
      val part = line.split(',')
      (part(0).toDouble,part(1).toDouble) //feature & data
    }).reduceByKey(_ + _)
    //**************Add noise**********************

    countresult.collect.foreach(println)

    spark.stop()
  }
}
