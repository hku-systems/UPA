package edu.hku.cs.dp

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.{MapPartitionsRDD, RDD, RDDOperationScope}

import scala.reflect.ClassTag


/**
  * Created by lionon on 10/22/18.
  */
class dpread[T: ClassTag](
  var rdd1 : RDD[T])
{
  var main = rdd1

  def mapDP[U: ClassTag](f: T => U, rate: Int): dpobject[U]= {
    val parameters = scala.io.Source.fromFile("security.csv").mkString.split(",")
    val t1 = System.nanoTime
    val advance_sampling = main.sparkContext.parallelize(main.take(rate))
    val sampling = main.sparkContext.parallelize(main.take(rate))
//    val sampling = main.sparkContext.parallelize(main.take(rate))
    val duration = (System.nanoTime - t1) / 1e9d
    println("sample: " + duration)
    println("sample size: " + rate)
    if(parameters(2).toInt == 1)// 1 means tune accuracy
      new dpobject(sampling.map(f), advance_sampling.map(f), main.subtract(sampling).map(f))
    else
      new dpobject(sampling.map(f), advance_sampling.map(f), main.map(f))
  }


    def mapDPKV[K: ClassTag,V: ClassTag](f: T => (K,V), rate: Int): dpobjectKV[K,V]= {
      val parameters = scala.io.Source.fromFile("security.csv").mkString.split(",")
      val t1 = System.nanoTime
      val advance_sampling = main.sparkContext.parallelize(main.take(rate))
      val sampling = main.sparkContext.parallelize(main.take(rate))
//      val sampling = main.sparkContext.parallelize(main.take(rate))
      val duration = (System.nanoTime - t1) / 1e9d
      println("sample: " + duration)
      println("sample size: " + rate)
      if(parameters(2).toInt == 1)// 1 means tune accuracy
        new dpobjectKV(sampling.map(f), advance_sampling.map(f), main.subtract(sampling).map(f))
      else
        new dpobjectKV(sampling.map(f), advance_sampling.map(f), main.map(f))
    }
}
