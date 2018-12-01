package org.apache.spark.examples

import edu.hku.cs.dp.dpread
import org.apache.spark.sql.SparkSession

import scala.math.random

/**
  * Created by lionon on 11/28/18.
  */
object TpchQuery1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("TpchQuery1")
      .getOrCreate()

    val count = new dpread(spark.read.textFile(args(0)).rdd)

    val countresult = count.mapDP(line => {
      val part = line.split(',').map(_.toInt)
      (part(0),part(1))
    }).filterDP(p => p._2 > 1.0).mapDPKV(q => q).reduceByKeyDP(_ + _)
    //**************Add noise**********************
    val original = countresult.original.collect()
    val broadcastValue = spark.sparkContext.broadcast(original.toMap)

    val newPointsSample = countresult.sample.map(p => {
      var OriginalValue = broadcastValue.value.getOrElse(p._1,p._2)//broadcast orignal result to each sample
      val diff = math.abs(OriginalValue - p._2)//First need to find the difference
      (p._1,diff)
    })

    val newPointsSampleMaxLocalSensitivity = newPointsSample.reduceByKey((a,b) => {
      math.max(a,b)
    })

    val localSensitivity = countresult.sample
    //********End of add noise**********************
    println("=====================Original : " + original)
    println("=====================Max local Sensitivity: ")
    newPointsSampleMaxLocalSensitivity.collect().foreach(n => {
      print(n._1 + "," + n._2)
    })
    spark.stop()
  }
}
