package edu.hku.dp

import org.apache.commons.math3.distribution.TDistribution
import org.apache.commons.math3.util.FastMath
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object ttest {
  def tTest(m1: Double, m2: Double, s1: Double, s2: Double, n1: Int, n2: Int): Double = {
    val t: Double = math.abs((m1 - m2) / FastMath.sqrt((s1 / n1) + (s2 / n2)))

    // pass a null rng to avoid unneeded overhead as we will not sample from this distribution
    val distribution: TDistribution = new TDistribution(null, n1 + n2 - 2)
    2.0 * distribution.cumulativeProbability(-t)
  }

  def main(args: Array[String]): Unit = {
    // this is used to implicitly convert an RDD to a DataFrame.
    val spark = SparkSession
      .builder
      .appName("Ttest")
      .getOrCreate()
//    val p_values = spark.sparkContext.textFile(args(0))
//      .map(_.split(','))
//      .map(p =>
//        (p(0).trim, p(1).trim.toInt, p(2).trim.toInt, p(3).trim.toDouble, p(4).trim.toDouble, p(5).trim.toInt))
//      //query name, dataset distance, self distance, mean, sd, count
//      .filter(_._11 < "1998-09-02")
//      .map(p => {
//        val inter = decrease(p._6,p._7)
//        inter
//      })
  }
}
