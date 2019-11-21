//package edu.hku.dp.test
//
//import edu.hku.cs.dp.dpread
//import org.apache.spark.sql.SparkSession
//
//object reduceDP {
//  def main(args: Array[String]) {
//    val spark = SparkSession
//      .builder
//      .appName("Spark Pi")
//      .getOrCreate()
//    val slices = if (args.length > 0) args(0).toInt else 2
////    val n = math.min(100000L * slices, Int.MaxValue).toInt // avoid overflow
//    val count = new dpread(spark.sparkContext.parallelize(1 until 1000, slices))
//      .mapDP( i => {
//        i*1.0
//      },50)
//      .reduce_and_add_noise_KDE(_ + _, "reduceDP",1)
//    println("Output is: " + count)
//    spark.stop()
//  }
//}
