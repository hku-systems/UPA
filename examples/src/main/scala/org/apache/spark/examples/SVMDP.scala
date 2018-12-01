package org.apache.spark.examples

import scala.collection.mutable.ArrayBuffer
import scala.math.signum
import edu.hku.cs.dp.dpread
import org.apache.spark.sql.SparkSession

/**
  * Created by lionon on 12/1/18.
  */
object SVMDP {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("SVMDP")
      .getOrCreate()

    val count = new dpread(spark.read.textFile(args(0)).rdd)

    val countresult = count.mapDPKV(line => {
      val part = line.split(',')
      (part(0).toDouble,part(1).toDouble) //feature & data
    }).reduceByKeyDP(_ + _)
    //**************Add noise**********************

    countresult.collect.foreach(println)

    spark.stop()
  }
}
