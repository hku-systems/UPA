//package edu.hku.dp.test
//
//import org.apache.spark.sql.SparkSession
//
//object reduceByKey {
//  def main(args: Array[String]) {
//    val spark = SparkSession
//      .builder
//      .appName("Spark Pi")
//      .getOrCreate()
//    val slices = if (args.length > 0) args(0).toInt else 2
////    val n = math.min(100000L * slices, Int.MaxValue).toInt // avoid overflow
//    val count = spark.sparkContext.parallelize(1 until 1000, slices)
//      .map{ i =>
//        (i%3, i*1.0)
//    }.reduceByKey(_ + _)
//    println("Output is: ")
//    count.foreach(println)
//    spark.stop()
//  }
//}
