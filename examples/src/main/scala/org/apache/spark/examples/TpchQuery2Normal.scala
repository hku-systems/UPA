package org.apache.spark.examples

import edu.hku.cs.dp.dpread
import org.apache.spark.sql.SparkSession

/**
  * Created by lionon on 11/28/18.
  */
object TpchQuery2Normal {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("TpchQuery2")
      .getOrCreate()

    //SELECT count (∗) FROM A JOIN B ON A .x > B.y

    val count = spark.read.textFile(args(0)).rdd

    val countresult1 = count.map(line => {
      val part = line.split(',').map(_.toInt)
      (part(0),part(1))
    }).map(q => q)
     val countresult = countresult1.join(countresult1).filter(ss => ss._2._1 > ss._2._2)
    //**************Add noise**********************
    val original = countresult.collect()
    original.foreach(println )
//    val broadcastValue = spark.sparkContext.broadcast(original.toMap)
//
//    val newPointsSample = countresult.sample.map(p => {
//      var OriginalValue = broadcastValue.value.getOrElse(p._1,p._2)//broadcast orignal result to each sample
//      val diff = math.abs(OriginalValue - p._2)//First need to find the difference
//      (p._1,diff)
//    })
//
//    val newPointsSampleMaxLocalSensitivity = newPointsSample.reduceByKey((a,b) => {
//      math.max(a,b)
//    })
//
//    val localSensitivity = countresult.sample
//    //********End of add noise**********************
//    println("=====================Original : " + original)
//    println("=====================Max local Sensitivity: ")
//    newPointsSampleMaxLocalSensitivity.collect().foreach(n => {
//      print(n._1 + "," + n._2)
//    })
    spark.stop()
  }
}
