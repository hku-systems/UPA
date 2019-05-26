package edu.hku.dp

import edu.hku.cs.dp.dpread
import org.apache.spark.sql.SparkSession

import scala.math.max

/**
 * TPC-H Query 1
 * Savvas Savvides <savvas@purdue.edu>
 *
 */
object TPCH21DP {

  def main(args: Array[String]): Unit = {
    // this is used to implicitly convert an RDD to a DataFrame.
    val spark = SparkSession
      .builder
      .appName("TpchQuery1")
      .getOrCreate()
    val inputDir = "/home/john/tpch-spark/dbgen"

    val supplier_input = new dpread(spark.sparkContext.textFile(inputDir + "/supplier.tbl*"),spark.sparkContext.textFile(inputDir + "/supplier.tbl*"))
      .mapDP(_.split('|'))
      .mapDPKV(p =>
        (p(3).trim.toLong, (p(0).trim.toLong, p(1).trim)))
    //(s_nationkey, (s_suppkey, s_name))

    //    val lineitem_input = spark.sparkContext.textFile(inputDir + "/lineitem.tbl*"))
    //      .mapDP(_.split('|'))
    //      .mapDP(p =>
    //      ( p(0).trim.toLong, (p(2).trim.toLong,  p(12).trim, p(11).trim, 1)))
    //    //(l_orderkey, (l_suppkey, l_receiptdate, l_commitdate, 1))
    //
    //    val lineitem4join = lineitem_input
    //      .mapDPKV(p => (p._2._1,(p._1,p._2._2,p._2._3,p._2._4)))
    //    //(l_suppkey,(l_orderkey, l_receiptdate, l_commitdate, 4))
    //
    //    val line1 = lineitem_input
    //      .mapDPKV(p=>p)
    //      .reduceByKeyDP((a,b) => (max(a._1, b._1), a._2, a._3, a._4 + b._4))
    //      .map(p => (p._1,(p._2._4,p._2._1)))
    //
    //    val order_input = new dpread(spark.sparkContext.textFile(inputDir + "/orders.tbl*"))
    //      .mapDP(_.split('|'))
    //      .mapDP(p =>
    //      (p(0).trim.toLong, p(2).trim))
    //      .filterDP(p => p._2 == "F")
    //      .mapDPKV(p => p)
    //    //(o_orderkey, o_orderstatus)
    //
    //    val nation_input = new dpread(spark.sparkContext.textFile(inputDir + "/nation.tbl*"))
    //      .mapDP(_.split('|'))
    //      .mapDP(p =>
    //      (p(0).trim.toLong, p(1).trim))
    //      .filterDP(p => p._2 == "SAUDI ARABIA")
    //      .mapDPKV(p => p)
    //      .joinDP(supplier_input)
    //    //(n_nationkey, (n_name,(s_suppkey,s_name)))
    //
    //    val supplierjoin = nation_input.mapDPKV(p => (p._2._2._1,p._2._2._2)).joinDP(lineitem4join)
    //    //(s_suppkey, (s_name,(l_orderkey, l_receiptdate, l_commitdate, 4)))
    //
    //    val orderkeyjoin = supplierjoin.mapDPKV(p => (p._2._2._1,(p._1,p._2._1))).joinDP(order_input)
    //    //(l_orderkey,((s_suppkey, s_name),o_orderstatus))
    //
    //    val line1join = orderkeyjoin.mapDPKV(p => p).joinDP(line1)
    //    //(l_orderkey,(((s_suppkey, s_name),o_orderstatus),(suppkey_count, max)))
    //
    //    val final_result = line1join
    //      .filterDP(p => p._2._2._1 > 1 || (p._2._2._1 == 1 && p._2._1._1 == p._2._2._2))
    //      .mapDP(p => (p._2._1._1._2, (p._1,p._2._1._1._1,p._2._2._1,p._2._2._2)))
    //      //$"s_name", ($"l_orderkey", $"l_suppkey", $"suppkey_count", $"suppkey_max")
    //      .filterDP(p => p._2._3 == 1 && p._2._2 == p._2._4)
    //      .mapDPKV(p => (p._1,1))
    //      .reduceByKeyDP((a,b) => a + b)
    //
    //    final_result.collect().foreach(p => print(p._1 + ":" + p._2 + "\n"))
    //
    //  }
  }
}
