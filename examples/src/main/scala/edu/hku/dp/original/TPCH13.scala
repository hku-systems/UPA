package edu.hku.dp

import edu.hku.cs.dp.dpread
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
      .appName("TpchQuery13")
      .getOrCreate()
    val inputDir = "/home/john/tpch-spark/dbgen"
    val t1 = System.nanoTime

    val customer_input = spark.sparkContext.textFile(args(0))
      .map(_.split('|'))
      .map(p =>
        (p(0).trim.toLong,1))

    val order_input = spark.sparkContext.textFile(args(1))
      .map(_.split('|'))
      .map(p =>
        (p(1).trim.toLong, p(0).trim.toLong))

    val final_result = customer_input
      .join(order_input)
      .map(p => 1.0)
      .reduce(_+_)

    val duration = (System.nanoTime - t1) / 1e9d
    println("Execution time: " + duration)
    spark.stop()
    //      .reduceByKeyDP_Int((a,b) => a + b,"TPCH13DP", args(4).toInt)
    //      .map(p => (p._2,1))
    //      .reduceByKey((a,b) => a + b)

    //    final_result.collect().foreach(p => print(p._1._1 + "," + p._1._2 + ":" + p._2 + "\n"))

  }
}
