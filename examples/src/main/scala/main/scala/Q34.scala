package main.scala

import org.apache.spark.sql.SparkSession

/**
 * TPC-H Query 1
 * Savvas Savvides <savvas@purdue.edu>
 *
 */
object Q31 extends {

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
      .appName("TpchQuery1")
      .getOrCreate()
    val inputDir = "/home/john/tpch-spark/dbgen"

    val order_filtered = spark.sparkContext.textFile(inputDir + "/orders.tbl*").map(_.split('|')).map(p =>
      (p(0).trim.toLong, (p(4).trim, p(5).trim))
      .filter(_._2._4 >= "1993-07-01" && _._2._4 < "1993-10-01")

    val lineitem_filtered = spark.sparkContext.textFile(inputDir + "/lineitem.tbl*").map(_.split('|')).map(p =>
      (p(0).trim.toLong, (p(1).trim.toLong, p(2).trim.toLong, p(3).trim.toLong, p(4).trim.toDouble, p(5).trim.toDouble, p(6).trim.toDouble, p(7).trim.toDouble, p(8).trim, p(9).trim, p(10).trim, p(11).trim, p(12).trim, p(13).trim, p(14).trim, p(15).trim)))
      .filter(_._2._11 < _._2._12).map(p => (p._1,1))

    val result = lineitem_filtered.join(order_filtered).map(p => (p._2._3,p._2._1)).reduceByKey((a,b)=> a + b)
      result.collect().foreach(println)

  }
}
