package edu.hku.dp.original

import org.apache.spark.sql.SparkSession

object TPCH11 {

  def mul(x: Double, y: Int):Double = {
    x * y
  }
  def mul0(x: Double): Double = {
    x * 0.0001
  }

  def main(args: Array[String]): Unit = {
    // this is used to implicitly convert an RDD to a DataFrame.
    val spark = SparkSession
      .builder
      .appName("TpchQuery11")
      .getOrCreate()
    val inputDir = "/home/john/tpch-spark/dbgen"

    val t1 = System.nanoTime

    val supplier_input = spark.sparkContext.textFile(args(0))
      .map(_.split('|'))
      .map(p =>
        (p(3).trim.toLong, p(0).trim.toLong))
    //s_nationkey, s_suppkey

    val nation_input = spark.sparkContext.textFile(args(1))
      .map(_.split('|'))
      .map(p =>
        (p(0).trim.toLong, p(1).trim))
      .filter(p => p._2 == "GERMANY")
      .map(p => p)
    //n_nationkey, n_name

    val nation_supplier = nation_input.join(supplier_input)
    //nationley, (n_name,s_suppkey)

    val partsupp_input = spark.sparkContext.textFile(args(2))
      .map(_.split('|'))
      .map(p =>
        (p(1).trim.toLong,p(2).trim.toInt * p(3).trim.toDouble))
    //PS_SUPPKEY, PS_AVAILQTY*PS_SUPPLYCOST

    val join_partsupp = nation_supplier
      .map(p => (p._2._2,p._1))
      .join(partsupp_input)

    val final_result = join_partsupp.map(p => p._2._2).reduce((a,b) => a + b)

    val duration = (System.nanoTime - t1) / 1e9d
    println("Execution time: " + duration)
    spark.stop()

  }
}
