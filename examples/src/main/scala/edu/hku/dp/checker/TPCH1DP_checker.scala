package edu.hku.dp.checker

import edu.hku.cs.dp.dpread_checker
import org.apache.spark.sql.SparkSession

/**
 * TPC-H Query 1
 * Savvas Savvides <savvas@purdue.edu>
 *
 */
object TPCH1DP_checker {

  def decrease(x: Double, y: Double): Double = {
    x * (1 - y)
  }

  def increase(x: Double, y: Double): Double = {
    x * (1 + y)
  }

  def removenan(r: Double): Double = {if (r.isNaN) Double.MinValue else r }

  def main(args: Array[String]): Unit = {
    // this is used to implicitly convert an RDD to a DataFrame.
    val spark = SparkSession
      .builder
      .appName("TpchQuery1-checker")
      .getOrCreate()
    val inputDir = "/home/john/tpch-dbgen/data/dbgen/"
    val t1 = System.nanoTime
    //    schemaProvider.lineitem.filter($"l_shipdate" <= "1998-09-02")
//      .groupBy($"l_returnflag", $"l_linestatus")
//      .agg(sum($"l_quantity"), sum($"l_extendedprice"),
//        sum(decrease($"l_extendedprice", $"l_discount")),
//        sum(increase(decrease($"l_extendedprice", $"l_discount"), $"l_tax")),
//        avg($"l_quantity"), avg($"l_extendedprice"), avg($"l_discount"), count($"l_quantity"))
//      .sort($"l_returnflag", $"l_linestatus")

    val filtered_result = new dpread_checker(spark.sparkContext.textFile(args(0)))
      .mapDP(_.split('|'),args(3).toInt)
      .mapDP(p =>
      (p(0).trim.toLong, p(1).trim.toLong, p(2).trim.toLong, p(3).trim.toLong, p(4).trim.toDouble, p(5).trim.toDouble, p(6).trim.toDouble, p(7).trim.toDouble, p(8).trim, p(9).trim, p(10).trim, p(11).trim, p(12).trim, p(13).trim, p(14).trim, p(15).trim))
      .filterDP(_._11 < "1998-09-02")
      .mapDP(p => {
//        val inter = decrease(p._6,p._7)
        1.0
      })

    filtered_result.reduceDP((a,b) => {
      a + b
    }) //arg 2 is the distance
    spark.stop()
  }
}
