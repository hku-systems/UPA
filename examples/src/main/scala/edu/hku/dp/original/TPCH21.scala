package edu.hku.dp

import edu.hku.cs.dp.dpread
import org.apache.spark.sql.SparkSession

import scala.math.max

/**
  * TPC-H Query 1
  * Savvas Savvides <savvas@purdue.edu>
  *
  */
object TPCH21 {

  def main(args: Array[String]): Unit = {
    // this is used to implicitly convert an RDD to a DataFrame.
    val spark = SparkSession
      .builder
      .appName("TpchQuery21")
      .getOrCreate()
    val inputDir = "/home/john/tpch-spark/dbgen"
    val t1 = System.nanoTime

    val supplier_input = spark.sparkContext.textFile(args(0))
      .map(_.split('|'))
      .map(p =>
        (p(3).trim.toLong, (p(0).trim.toLong, p(1).trim)))
    //(s_nationkey, (s_suppkey, s_name))

    val lineitem_input = spark.sparkContext.textFile(args(1))
      .map(_.split('|'))
      .map(p =>
        ( p(0).trim.toLong, (p(2).trim.toLong,  p(12).trim, p(11).trim, 1)))
    //(l_orderkey, (l_suppkey, l_receiptdate, l_commitdate, 1))

    val lineitem4join = lineitem_input
      .map(p => (p._2._1,(p._1,p._2._2,p._2._3,p._2._4)))
    //(l_suppkey,(l_orderkey, l_receiptdate, l_commitdate, 4))

    val line1 = spark.sparkContext.textFile(args(2))
      .map(_.split('|'))
      .map(p =>
        ( p(0).trim.toLong, (p(2).trim.toLong,  p(12).trim, p(11).trim, 1)))
      .reduceByKey((a,b) => (max(a._1, b._1), a._2, a._3, a._4 + b._4))
      .map(p => (p._1,(p._2._4,p._2._1)))

    val order_input = spark.sparkContext.textFile(args(3))
      .map(_.split('|'))
      .map(p =>
        (p(0).trim.toLong, p(2).trim))
      .filter(p => p._2 == "F")
      .map(p => p)
    //(o_orderkey, o_orderstatus)

    val nation_input = spark.sparkContext.textFile(args(4))
      .map(_.split('|'))
      .map(p =>
        (p(0).trim.toLong, p(1).trim))
      .filter(p => p._2 == "SAUDI ARABIA")
      .map(p => p)

    val nation_join = nation_input.join(supplier_input)
    //          .joinDP(supplier_input)
    //(n_nationkey, (n_name,(s_suppkey,s_name)))

    val supplierjoin = nation_join.map(p => (p._2._2._1,p._2._2._2)).join(lineitem4join)
    //(s_suppkey, (s_name,(l_orderkey, l_receiptdate, l_commitdate, 4)))

    val orderkeyjoin = supplierjoin.map(p => (p._2._2._1,(p._1,p._2._1))).join(order_input)
    //(l_orderkey,((s_suppkey, s_name),o_orderstatus))

    val line1join = orderkeyjoin.map(p => p).join(line1)
    //(l_orderkey,(((s_suppkey, s_name),o_orderstatus),(suppkey_count, max)))

    val final_result_before_reduce = line1join
      .filter(p => p._2._2._1 > 1 || (p._2._2._1 == 1 && p._2._1._1 == p._2._2._2))
      .map(p => (p._2._1._1._2, (p._1,p._2._1._1._1,p._2._2._1,p._2._2._2)))
      //          //$"s_name", ($"l_orderkey", $"l_suppkey", $"suppkey_count", $"suppkey_max")
      //          .filterDP(p => p._2._3 == 1 && p._2._2 == p._2._4)
      .map(p => 1.0)
    if(args(5) == "0" || args(5) == "2") {
      val final_result = final_result_before_reduce.reduce((a, b) => a + b)
    }
    else if(args(5) == "1" || args(5) == "2") { //1 means print length
      val result_length = final_result_before_reduce.count()
      print("result_length: " + result_length)
    }
    val duration = (System.nanoTime - t1) / 1e9d
    println("Execution time: " + duration)
    spark.stop()
  }
}
