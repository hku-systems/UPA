package main.scala

import org.apache.spark.sql.SparkSession

/**
 * TPC-H Query 1
 * Savvas Savvides <savvas@purdue.edu>
 *
 */
object Q46 {

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

    val part_input = spark.sparkContext.textFile(inputDir + "/part.tbl*").map(_.split('|')).map(p =>
      (p(0).trim.toLong, (p(3).trim, p(4).trim, p(5).trim.toLong)))
      .filter(p => p._2._1 != "Brand#45" && !polished(p._2._2) && numbers(p._2._3)).map(p => p)


    val supplier_input = spark.sparkContext.textFile(inputDir + "/supplier.tbl*").map(_.split('|')).map(p =>
      (p(0).trim.toLong, p(6).trim))
      .filter(p => !complains(p._2))

    val partsupp_input = spark.sparkContext.textFile(inputDir + "/partsupp.tbl*").map(_.split('|')).map(p =>
      ( p(1).trim.toLong,p(0).trim.toLong))

    val final_result = supplier_input.join(partsupp_input)
      .map(p => (p._2._2,p._1))
      .join(part_input)
      .map(p => ((p._2._2._1,p._2._2._2,p._2._2._3),1)).reduceByKey((a,b) => a + b)

    final_result.collect().foreach(println)
  }
}
