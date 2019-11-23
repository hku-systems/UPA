package edu.hku.dp.checker

import edu.hku.cs.dp.dpread_checker
import org.apache.spark.sql.SparkSession

object TPCH11DP_checker {

  def main(args: Array[String]): Unit = {
    // this is used to implicitly convert an RDD to a DataFrame.
    val spark = SparkSession
      .builder
      .appName("TpchQuery11DP_checker")
      .getOrCreate()
    val inputDir = "/home/john/tpch-spark/dbgen"
    val t1 = System.nanoTime

    val supplier_input = spark.sparkContext.textFile(args(0))
      .map(_.split('|'))
      .map(p =>
        (p(3).trim.toLong, p(0).trim.toLong))
    //s_nationkey, s_suppkey

    val nation_input = spark.sparkContext.textFile(args(2))
      .map(_.split('|'))
      .map(p =>
        (p(0).trim.toLong, p(1).trim))
      .filter(p => p._2 == "GERMANY")
      .map(p => p)
    //n_nationkey, n_name

    val nation_supplier = supplier_input.join(nation_input)
    //nationley, (n_name,s_suppkey)

    val partsupp_input = new dpread_checker(spark.sparkContext.textFile(args(3)))
      .mapDP(_.split('|'),args(6).toInt)
      .mapDPKV(p => {
        (p(1).trim.toLong,p(2).trim.toInt * p(3).trim.toDouble)
      })
    //PS_SUPPKEY, PS_AVAILQTY*PS_SUPPLYCOST

    val join_partsupp = partsupp_input
      .joinDP(nation_supplier.map(p => (p._1,p._2._2)))

    join_partsupp
      .mapDP(p => p._2._2)
      .reduceDP((a,b) => a + b)

    spark.stop()
  }
}
