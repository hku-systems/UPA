package edu.hku.dp.checker

import edu.hku.cs.dp.dpread_checker
import org.apache.spark.sql.SparkSession

import scala.math.max

/**
 * TPC-H Query 1
 * Savvas Savvides <savvas@purdue.edu>
 *
 */
object TPCH21DP_checker {

  def main(args: Array[String]): Unit = {
    // this is used to implicitly convert an RDD to a DataFrame.
    val spark = SparkSession
      .builder
      .appName("TpchQuery21DP")
      .getOrCreate()
    val inputDir = "/home/john/tpch-spark/dbgen"
    val t1 = System.nanoTime

    val supplier_input = spark.sparkContext.textFile(args(0))
      .map(_.split('|'))
      .map(p =>
        (p(3).trim.toLong, (p(0).trim.toLong, p(1).trim)))
    //(s_nationkey, (s_suppkey, s_name))

        val lineitem4join = new dpread_checker(spark.sparkContext.textFile(args(2)))
          .mapDPKV(p => {
            val s = p.split('|')
            (s(0).trim.toLong, (s(2).trim.toLong,  s(12).trim, s(11).trim, 1))
          },args(8).toInt)
        //(l_orderkey, (l_suppkey, l_receiptdate, l_commitdate, 1))

        val line1 = spark.sparkContext.textFile(args(3))
          .map(_.split('|'))
          .map(p =>
            ( p(0).trim.toLong, (p(2).trim.toLong,  p(12).trim, p(11).trim, 1)))
          .reduceByKey((a,b) => (max(a._1, b._1), a._2, a._3, a._4 + b._4))
          .map(p => (p._1,(p._2._4,p._2._1)))

        val order_input = spark.sparkContext.textFile(args(4))
          .map(_.split('|'))
          .map(p =>
          (p(0).trim.toLong, p(2).trim))
          .filter(p => p._2 == "F")
        //(o_orderkey, o_orderstatus)

        val nation_input = spark.sparkContext.textFile(args(6))
          .map(_.split('|'))
          .map(p =>
          (p(0).trim.toLong, p(1).trim))
          .filter(p => p._2 == "SAUDI ARABIA")

        val nation_join = nation_input.join(supplier_input)
//          .joinDP(supplier_input)
        //(n_nationkey, (n_name,(s_suppkey,s_name)))

        val supplierjoin = lineitem4join.joinDP(nation_join.map(p => (p._2._2._1,p._2._2._2)))
        //(s_suppkey, (s_name,(l_orderkey, l_receiptdate, l_commitdate, 4)))

        val orderkeyjoin = supplierjoin.mapDPKV(p => (p._2._2._1,(p._1,p._2._1))).joinDP_original(order_input)
        //(l_orderkey,((s_suppkey, s_name),o_orderstatus))

        val line1join = orderkeyjoin.mapDPKV(p => p).joinDP_original(line1)
        //(l_orderkey,(((s_suppkey, s_name),o_orderstatus),(suppkey_count, max)))

        val final_result_before_reduce = line1join
          .filterDP(p => p._2._2._1 > 1 || (p._2._2._1 == 1 && p._2._1._1 == p._2._2._2))
          .mapDP(p => (p._2._1._1._2, (p._1,p._2._1._1._1,p._2._2._1,p._2._2._2)))
//          //$"s_name", ($"l_orderkey", $"l_suppkey", $"suppkey_count", $"suppkey_max")
//          .filterDP(p => p._2._3 == 1 && p._2._2 == p._2._4)
          .mapDP(p => 1.0)

      final_result_before_reduce.reduceDP((a, b) => a + b)

    spark.stop()
      }
  }

