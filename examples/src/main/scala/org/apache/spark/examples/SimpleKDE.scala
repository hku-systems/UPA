package org.apache.spark.examples

import org.apache.spark.sql.SparkSession

import scala.math.{exp,sqrt,Pi}

object SimpleKDE {

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

    val inputPath = args(0)
    val lines = spark.read.textFile(inputPath).rdd

    val points = lines
      .map(p => {
        Kernel2(phi(p.toDouble))
    }).reduce(_+_)

    println(points)
    spark.stop
  }
}
