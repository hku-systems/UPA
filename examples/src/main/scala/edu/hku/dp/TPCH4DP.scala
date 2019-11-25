package edu.hku.dp

import edu.hku.cs.dp.dpread
import org.apache.spark.sql.SparkSession

/**
 * TPC-H Query 1
 * Savvas Savvides <savvas@purdue.edu>
 *
 */
object TPCH4DP {

  def main(args: Array[String]): Unit = {
    // this is used to implicitly convert an RDD to a DataFrame.
    val input_size = args(2).split('.').last
    val spark = SparkSession
      .builder
      .appName("TpchQuery4DP-" + input_size + "_" + args(5))
      .getOrCreate()
    val inputDir = "/home/john/tpch-spark/dbgen"
    val t1 = System.nanoTime

    val order_filtered = spark.sparkContext.textFile(args(0))
      .map(_.split('|'))
      .map(p =>
      (p(0).trim.toLong, (p(4).trim, p(5).trim)))
      .filter(p => p._2._1 >= "1993-07-01" && p._2._1 < "1993-10-01")

    val lineitem_filtered = new dpread(spark.sparkContext.textFile(args(2)))
      .mapDP(_.split('|'),args(5).toInt)
      .mapDP(p =>
      (p(0).trim.toLong, (p(11).trim, p(12).trim,p(4).trim.toDouble)))
      .filterDP(p => p._2._1 < p._2._2)
      .mapDPKV(p => (p._1,p._2._3))

    val result = lineitem_filtered
      .joinDP_original(order_filtered)
      .mapDP(p => 1.0).reduceDP(_+_, args(4).toInt)
//      .reduceByKeyDP_Int((a,b)=> a + b)
val duration = (System.nanoTime - t1) / 1e9d
    println("final output: " + result._1)
    println("noise: " + result._2)
    println("error: " + result._2/result._1)
    println("min bound: " + result._3)
    println("max bound: " + result._4)
    println("Execution time: " + duration)
    spark.stop()
    //    result.collect().foreach(p => print(p._1._1 + "," + p._1._2 + ":" + p._2 + "\n"))
  }
}
