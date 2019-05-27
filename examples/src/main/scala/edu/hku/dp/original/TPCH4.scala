package edu.hku.dp.original

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
      .appName("TpchQuery1")
      .getOrCreate()
    val inputDir = "/home/john/tpch-spark/dbgen/ground-truth"

    val order_filtered = spark.sparkContext.textFile(args(0)).map(_.split('|')).map(p =>
      (p(0).trim.toLong, (p(4).trim, p(5).trim)))
      .filter(p => p._2._1 >= "1993-07-01" && p._2._1 < "1993-10-01")

    val lineitem_filtered = spark.sparkContext.textFile(args(1)).map(_.split('|')).map(p =>
      (p(0).trim.toLong, (p(11).trim, p(12).trim)))
      .filter(p => p._2._1 < p._2._2).map(p => (p._1,1))

    val result = lineitem_filtered.join(order_filtered).map(p => (p._2._2,p._2._1)).reduceByKey((a,b)=> a + b)
      result.collect().foreach(p => print(args(0) + ":" + p._1._1 + "," + p._1._2 + ":" + p._2 + "\n"))
  }
}
