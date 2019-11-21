package edu.hku.dp

import edu.hku.cs.dp.dpread
import org.apache.spark.sql.SparkSession

/**
  * TPC-H Query 1
  * Savvas Savvides <savvas@purdue.edu>
  *
  */
object TPCH4 {

  def main(args: Array[String]): Unit = {
    // this is used to implicitly convert an RDD to a DataFrame.
    val spark = SparkSession
      .builder
      .appName("TpchQuery4")
      .getOrCreate()
    val inputDir = "/home/john/tpch-spark/dbgen"

    val t1 = System.nanoTime

    val order_filtered = spark.sparkContext.textFile(args(0))
      .map(_.split('|'))
      .map(p =>
        (p(0).trim.toLong, (p(4).trim, p(5).trim)))
      .filter(p => p._2._1 >= "1993-07-01" && p._2._1 < "1993-10-01")

    val lineitem_filtered = spark.sparkContext.textFile(args(1))
      .map(_.split('|'))
      .map(p =>
        (p(0).trim.toLong, (p(11).trim, p(12).trim,p(4).trim.toDouble)))
      .filter(p => p._2._1 < p._2._2)
      .map(p => (p._1,p._2._3))

    val result = lineitem_filtered
      .join(order_filtered)
      .map(p => 1.0).reduce(_+_)
    //      .reduceByKeyDP_Int((a,b)=> a + b)
    val duration = (System.nanoTime - t1) / 1e9d
    println("Execution time: " + duration)
    spark.stop()    //    result.collect().foreach(p => print(p._1._1 + "," + p._1._2 + ":" + p._2 + "\n"))
  }
}
