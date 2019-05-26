package edu.hku.dp

import edu.hku.cs.dp.dpread
import org.apache.spark.sql.SparkSession

import scala.math.{Pi, exp, sqrt}

object SimpleKDEDP {

  def phi(x:Double):Double = {
    exp(-0.5*x*x)/sqrt(2*Pi)
  }

  def tpdf(x: Double): Double = {
    phi(x + 10)/2 + phi(x - 10)/2
  }

  def Kernel2(x: Double): Double = {
    phi(x) / 1.42
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("Kernel Density Estimation")
      .getOrCreate()

    val lines = new dpread(spark.sparkContext.textFile(args(0)),spark.sparkContext.textFile(args(1)))

    val points = lines
      .mapDP(p => {
        Kernel2(phi(p.toDouble))
    }).reduce_and_add_noise_KDE(_+_)

    println("Result: " + points)
//    val final_result = points.collect()
//    println(final_result)
    spark.stop
  }
}
