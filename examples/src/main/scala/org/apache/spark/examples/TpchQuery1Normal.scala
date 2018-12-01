package org.apache.spark.examples

import edu.hku.cs.dp.dpread
import org.apache.spark.sql.SparkSession

/**
  * Created by lionon on 11/28/18.
  */
object TpchQuery1Normal {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("TpchQuery1")
      .getOrCreate()

    val count = spark.read.textFile(args(0)).rdd

    val countresult = count.map(line => {
      val part = line.split(',').map(_.toInt)
      (part(0),part(1))
    }).filter(p => p._2 > 1.0).map(q => q).reduceByKey(_ + _)
    //**************Add noise**********************
    val original = countresult.collect()
//    val broadcastValue = spark.sparkContext.broadcast(original.toMap)

//    val newPointsSample = countresult.sample.map(p => {
//      var OriginalValue = broadcastValue.value.getOrElse(p._1,p._2)//broadcast orignal result to each sample
//      val diff = math.abs(OriginalValue - p._2)//First need to find the difference
//      (p._1,diff)
//    })
//
//    val newPointsSampleMaxLocalSensitivity = newPointsSample.reduceByKey((a,b) => {
//      math.max(a,b)
//    })

//    val localSensitivity = countresult.sample
    //********End of add noise**********************
    println("=====================Original : " + original)
//    println("=====================Max local Sensitivity: ")
//    newPointsSampleMaxLocalSensitivity.collect().foreach(n => {
//      print(n._1 + "," + n._2)
//    })
    spark.stop()
  }
}
