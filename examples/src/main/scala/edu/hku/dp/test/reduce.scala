//package edu.hku.dp.test
//
//import edu.hku.cs.dp.dpread
//import org.apache.spark.sql.SparkSession
//
//object reduce {
//  def main(args: Array[String]) {
//    val spark = SparkSession
//      .builder
//      .appName("Spark Pi")
//      .getOrCreate()
//    val slices = if (args.length > 0) args(0).toInt else 2
////    val n = math.min(100000L * slices, Int.MaxValue).toInt // avoid overflow
//    val count = spark.sparkContext.parallelize(1 until 1000, slices)
//      .map{ i =>
//      i*1.0
//    }.reduce(_ + _)
//    println("Output is: " + count)
//    spark.stop()
//  }
//}
