package edu.hku.dp

import edu.hku.cs.dp.dpread
import org.apache.spark.sql.SparkSession

/**
 * TPC-H Query 1
 * Savvas Savvides <savvas@purdue.edu>
 *
 */
object TPCH13DP {

  def special(x: String): Boolean = {
    x.matches(".*special.*requests.*")
  }

  def main(args: Array[String]): Unit = {
    // this is used to implicitly convert an RDD to a DataFrame.
    val input_size = args(0).split('.').last
    val spark = SparkSession
      .builder
      .appName("TpchQuery13DP-" + input_size + "-" + args(5))
      .getOrCreate()
    val inputDir = "/home/john/tpch-spark/dbgen"
    val t1 = System.nanoTime

    val lineitem = new dpread(spark.sparkContext.textFile(args(0)))
      .mapDP(_.split('|'),args(5).toInt)
      .mapDPKV(p =>
      (p(0).trim.toLong,1))

    val order_input = spark.sparkContext.textFile(args(2))
      .map(_.split('|'))
      .map(p =>
      (p(1).trim.toLong, p(0).trim.toLong))

    val final_result = lineitem
      .joinDP_original(order_input)
      .mapDP(p => 1.0)
      .reduceDP(_+_, args(4).toInt)

    val duration = (System.nanoTime - t1) / 1e9d
    println("final output: " + final_result._1)
    println("noise: " + final_result._2)
    println("error: " + final_result._2/final_result._1)
    println("min bound: " + final_result._3)
    println("max bound: " + final_result._4)
    println("Execution time: " + duration)
    spark.stop()

  }
}
