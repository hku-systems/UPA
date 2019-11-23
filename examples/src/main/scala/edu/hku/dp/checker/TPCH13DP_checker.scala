package edu.hku.dp.checker

import edu.hku.cs.dp.dpread_checker
import org.apache.spark.sql.SparkSession

/**
 * TPC-H Query 1
 * Savvas Savvides <savvas@purdue.edu>
 *
 */
object TPCH13DP_checker {

  def special(x: String): Boolean = {
    x.matches(".*special.*requests.*")
  }

  def main(args: Array[String]): Unit = {
    // this is used to implicitly convert an RDD to a DataFrame.
    val spark = SparkSession
      .builder
      .appName("TpchQuery13DP")
      .getOrCreate()
    val inputDir = "/home/john/tpch-spark/dbgen"
    val t1 = System.nanoTime

    val lineitem = new dpread_checker(spark.sparkContext.textFile(args(0)))
      .mapDP(_.split('|'),args(5).toInt)
      .mapDPKV(p =>
      (p(0).trim.toLong,1))

    val order_input = spark.sparkContext.textFile(args(2))
      .map(_.split('|'))
      .map(p =>
      (p(1).trim.toLong, p(0).trim.toLong))

    lineitem
      .joinDP_original(order_input)
      .mapDP(p => 1.0)
      .reduceDP(_+_)

    spark.stop()

  }
}
