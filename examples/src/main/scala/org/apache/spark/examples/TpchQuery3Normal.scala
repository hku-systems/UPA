package org.apache.spark.examples

import edu.hku.cs.dp.dpread
import org.apache.spark.sql.SparkSession

/**
  * Created by lionon on 11/28/18.
  */
object TpchQuery3Normal {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("TpchQuery3")
      .getOrCreate()

    val count = spark.read.textFile(args(0)).rdd

//    WITH A AS ( SELECT count (∗) FROM T1 ),
//    B AS ( SELECT count (∗) FROM T2 )
//    SELECT count (∗) FROM A JOIN B ON A . count = B. count

    val countresult1 = count.map(line => {
      val part = line.toInt
      (part,1)
    }).map(q => q).reduceByKey((a,b) => a + b).map(_.swap)
     val countresult = countresult1.join(countresult1)
    //**************Add noise**********************
    val original = countresult.collect()
    original.foreach(println)
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
