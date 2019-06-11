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
    val spark = SparkSession
      .builder
      .appName("TpchQuery1")
      .getOrCreate()
    val inputDir = "/home/john/tpch-spark/dbgen"
    val t1 = System.nanoTime

    val customer_input = new dpread(spark.sparkContext.textFile(args(0)))
      .mapDP(_.split('|'),args(5).toInt)
      .mapDPKV(p =>
      (p(0).trim.toLong,1))

    val order_input = new dpread(spark.sparkContext.textFile(args(2)))
      .mapDP(_.split('|'),args(5).toInt)
      .mapDPKV(p =>
      (p(1).trim.toLong, p(0).trim.toLong))

    val final_result = customer_input
      .joinDP(order_input)
      .mapDP(p => 1.0)
      .reduce_and_add_noise_KDE(_+_,"TPCH13DP", args(4).toInt)

    val duration = (System.nanoTime - t1) / 1e9d
    println("Execution time: " + duration)
    spark.stop()

  }
}
