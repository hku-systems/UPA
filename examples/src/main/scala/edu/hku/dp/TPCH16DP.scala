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
    val input_size = args(4).split('.').last
    val spark = SparkSession
      .builder
      .appName("TpchQuery16DP-" + input_size + "-" + args(7))
      .getOrCreate()
    val inputDir = "/home/john/tpch-spark/dbgen"
    val t1 = System.nanoTime

    val part_input = spark.sparkContext.textFile(args(0))
      .map(_.split('|'))
      .map(p =>
      (p(0).trim.toLong, (p(3).trim, p(4).trim, p(5).trim.toLong)))
      .filter(p => p._2._1 != "Brand#45" && !polished(p._2._2) && numbers(p._2._3))

    val supplier_input = spark.sparkContext.textFile(args(2))
      .map(_.split('|'))
      .map(p =>
      (p(0).trim.toLong, p(6).trim))
      .filter(p => !complains(p._2))

    val partsupp_input = new dpread(spark.sparkContext.textFile(args(4)))
      .mapDP(_.split('|'),args(7).toInt)
      .mapDPKV(p =>
      ( p(1).trim.toLong,p(0).trim.toLong))

    val final_result = partsupp_input.joinDP(supplier_input)
      .mapDPKV(p => (p._2._2,p._1))
      .joinDP(part_input)
      .mapDP(p => 1.0)
      .reduceDP((a,b) => a + b, args(6).toInt)
//      .reduceByKeyDP_Int((a,b) => a + b,"TPCH16DP", args(6).toInt)
val duration = (System.nanoTime - t1) / 1e9d
    println("final output: " + final_result._1)
    println("noise: " + final_result._2)
    println("error: " + final_result._2/final_result._1)
    println("min bound: " + final_result._3)
    println("max bound: " + final_result._4)
    println("Execution time: " + duration)
    spark.stop()
//    final_result.collect().foreach(p => print(p._1._1 + "," + p._1._2 + p._1._3 +  ":" + p._2 + "\n"))
  }
}
