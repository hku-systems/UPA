package edu.hku.dp.original

import org.apache.spark.sql.SparkSession

/**
 * TPC-H Query 1
 * Savvas Savvides <savvas@purdue.edu>
 *
 */
object TPCH13 {

  def special(x: String): Boolean = {
    x.matches(".*special.*requests.*")
  }

  def main(args: Array[String]): Unit = {
    // this is used to implicitly convert an RDD to a DataFrame.
    val spark = SparkSession
      .builder
      .appName("TpchQuery1")
      .getOrCreate()
    val inputDir = "/home/john/tpch-spark/dbgen/ground-truth"

    val customer_input = spark.sparkContext.textFile(inputDir + "/customer"+ args(0) +".ftbl*").map(_.split('|')).map(p =>
      (p(0).trim.toLong,1))

    val order_input = spark.sparkContext.textFile(inputDir + "/orders"+ args(0) +".ftbl*").map(_.split('|')).map(p =>
      (p(1).trim.toLong, p(0).trim.toLong))

    val final_result = customer_input
      .join(order_input)
      .map(p => ((p._1,p._2._2),p._2._1))
      .reduceByKey((a,b) => a + b)
//      .map(p => (p._2,1))
//      .reduceByKey((a,b) => a + b)

    final_result.collect().foreach(p => print(args(0) + ":" + p._1._1 + "," + p._1._2 + ":" + p._2 + "\n"))

  }
}
