package edu.hku.dp

import edu.hku.cs.dp.dpread
import org.apache.spark.sql.SparkSession

object TPCH6DP {
  def main(args: Array[String]): Unit = {
    // this is used to implicitly convert an RDD to a DataFrame.
    val spark = SparkSession
      .builder
      .appName("TpchQuery6")
      .getOrCreate()
    val inputDir = "/home/john/tpch-spark/dbgen"
    //    schemaProvider.lineitem.filter($"l_shipdate" <= "1998-09-02")
    //      .groupBy($"l_returnflag", $"l_linestatus")
    //      .agg(sum($"l_quantity"), sum($"l_extendedprice"),
    //        sum(decrease($"l_extendedprice", $"l_discount")),
    //        sum(increase(decrease($"l_extendedprice", $"l_discount"), $"l_tax")),
    //        avg($"l_quantity"), avg($"l_extendedprice"), avg($"l_discount"), count($"l_quantity"))
    //      .sort($"l_returnflag", $"l_linestatus")

    val filtered_result = new dpread(spark.sparkContext.textFile(args(0)),spark.sparkContext.textFile(args(1)))
      .mapDP(_.split('|'))
      .mapDP(p =>
        (p(0).trim.toLong, p(1).trim.toLong, p(2).trim.toLong, p(3).trim.toLong, p(4).trim.toDouble, p(5).trim.toDouble, p(6).trim.toDouble, p(7).trim.toDouble, p(8).trim, p(9).trim, p(10).trim, p(11).trim, p(12).trim, p(13).trim, p(14).trim, p(15).trim))
      //      .map(case (l_orderkey: Long, l_partkey: Long, l_suppkey: Long, l_linenumber: Long, l_quantity: Double, l_extendedprice: Double, l_discount:Double, l_tax:Double, l_returnflag:String, l_linestatus:String, l_shipdate:String, l_commitdate:String, l_receiptdate:String, l_shipinstruct:String, l_shipmode:String, l_comment:String))
      .filterDP(p => p._11 >= "1994-01-01" && p._11 < "1995-01-01" && p._7 <= 0.07 &&  p._5 < 24)
      .mapDP(p => p._6*p._7).reduce_and_add_noise_KDE((a,b) => a + b, "TPCH6DP", args(2).toInt)

    //    println("filtered_result original")
    //    filtered_result.original.collect().foreach(println)

    println("Output: " + filtered_result)

    //    final_result.collect().foreach(p => print(p._1._1 + "," + p._1._2 + ":" + p._2._1 + "," + p._2._2 + "," + p._2._3 + "," + p._2._4 + "," + p._2._5 + "," + p._2._6 + "\n"))

  }
}
