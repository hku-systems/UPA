package main.scala

import edu.hku.cs.dp.dpread
import org.apache.spark.sql.SparkSession

/**
 * TPC-H Query 1
 * Savvas Savvides <savvas@purdue.edu>
 *
 */
object Q34DP {

  def main(args: Array[String]): Unit = {
    // this is used to implicitly convert an RDD to a DataFrame.
    val spark = SparkSession
      .builder
      .appName("TpchQuery1")
      .getOrCreate()
    val inputDir = "/home/john/tpch-spark/dbgen"

    val order_filtered = new dpread(spark.sparkContext.textFile(inputDir + "/orders.tbl*"))
      .mapDP(_.split('|'))
      .mapDPKV(p =>
      (p(0).trim.toLong, (p(4).trim, p(5).trim)))
      .filterDPKV(p => p._2._1 >= "1993-07-01" && p._2._1 < "1993-10-01")

    val lineitem_filtered = new dpread(spark.sparkContext.textFile(inputDir + "/lineitem.tbl*"))
      .mapDP(_.split('|'))
      .mapDP(p =>
      (p(0).trim.toLong, (p(11).trim, p(12).trim)))
      .filterDP(p => p._2._1 < p._2._2)
      .mapDPKV(p => (p._1,1))

    val result = lineitem_filtered
      .joinDP(order_filtered)
      .mapDPKV(p => (p._2._2,p._2._1))
      .reduceByKeyDP((a,b)=> a + b)

      result.addnoiseQ34()
  }
}
