package edu.hku.dp

import edu.hku.cs.dp.dpread
import org.apache.spark.sql.SparkSession

/**
 * TPC-H Query 1
 * Savvas Savvides <savvas@purdue.edu>
 *
 */
object TPCH16DP {

  def decrease(x: Double, y: Double): Double = {
  x * (1 - y)
}
  def complains(x: String) : Boolean = {
  x.matches (".*Customer.*Complaints.*")
}

  def polished(x: String) : Boolean = {
    x.startsWith("MEDIUM POLISHED")
  }

  def numbers(x: Long) : Boolean = {
    x.toString().matches("49|14|23|45|19|3|36|9")
  }

  def main(args: Array[String]): Unit = {
    // this is used to implicitly convert an RDD to a DataFrame.
    val spark = SparkSession
      .builder
      .appName("TpchQuery1")
      .getOrCreate()
    val inputDir = "/home/john/tpch-spark/dbgen"

    val part_input = new dpread(spark.sparkContext.textFile(inputDir + "/part.tbl*"))
      .mapDP(_.split('|'))
      .mapDP(p =>
      (p(0).trim.toLong, (p(3).trim, p(4).trim, p(5).trim.toLong)))
      .filterDP(p => p._2._1 != "Brand#45" && !polished(p._2._2) && numbers(p._2._3)).mapDPKV(p => p)


    val supplier_input = new dpread(spark.sparkContext.textFile(inputDir + "/supplier.tbl*"))
      .mapDP(_.split('|'))
      .mapDP(p =>
      (p(0).trim.toLong, p(6).trim))
      .filterDP(p => !complains(p._2))
      .mapDPKV(p => p)

    val partsupp_input = new dpread(spark.sparkContext.textFile(inputDir + "/partsupp.tbl*"))
      .mapDP(_.split('|'))
      .mapDPKV(p =>
      ( p(1).trim.toLong,p(0).trim.toLong))

    val final_result = supplier_input.joinDP(partsupp_input)
      .mapDPKV(p => (p._2._2,p._1))
      .joinDP(part_input)
      .mapDPKV(p => ((p._2._2._1,p._2._2._2,p._2._2._3),1))
      .reduceByKeyDP((a,b) => a + b)

    final_result.addnoiseQ46()
  }
}
