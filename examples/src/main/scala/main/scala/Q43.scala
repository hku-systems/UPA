package main.scala

import org.apache.spark.sql.SparkSession

/**
 * TPC-H Query 1
 * Savvas Savvides <savvas@purdue.edu>
 *
 */
object Q43 {

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

    val customer_input = spark.sparkContext.textFile(inputDir + "/customer.tbl*").map(_.split('|')).map(p =>
      (p(0).trim.toLong,1))

    val order_input = spark.sparkContext.textFile(inputDir + "/orders.tbl*").map(_.split('|')).map(p =>
      (p(1).trim.toLong, p(0).trim.toLong))

    val final_result = customer_input
      .join(order_input)
      .map(p => ((p._1,p._2._2),p._2._1))
      .reduceByKey((a,b) => a + b)
//      .map(p => (p._2,1))
//      .reduceByKey((a,b) => a + b)

    final_result.collect().foreach(println)

  }
}
