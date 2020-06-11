package edu.hku.cs.dp

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.{MapPartitionsRDD, RDD, RDDOperationScope}

import scala.reflect.ClassTag


/**
  * Created by lionon on 10/22/18.
  */
class dpread_c[T: ClassTag](
  var rdd1 : RDD[String])
{
  var main = rdd1.map(p => {
    val s = p.split(';')
    (s(0),s(1).trim.toLong)
  })

  def mapDP[U: ClassTag](f: String => U, rate: Int): dpobject_c[U]= {
    val parameters = scala.io.Source.fromFile("security.csv").mkString.split(",")
    val t1 = System.nanoTime
    val advance_sampling = main.sparkContext.parallelize(main.take(rate))
    val sampling = main.sparkContext.parallelize(main.take(rate))
//    val sampling = main.sparkContext.parallelize(main.take(rate))
    val duration = (System.nanoTime - t1) / 1e9d
//    if(parameters(2).toInt == 1)// 1 means tune accuracy
    new dpobject_c(sampling.map(p => (f(p._1),p._2)), advance_sampling.map(p => f(p._1)), main.subtract(sampling).map(p => (f(p._1),p._2)))
//    else
//      new dpobject_c(sampling.map(p => (f(p._1),p._2)), advance_sampling.map(p => f(p._1)), main.map(p => (f(p._1),p._2)))
  }


//    def mapDPKV[K: ClassTag,V: ClassTag](f: String => (K,V), rate: Int): dpobjectKV_c[K,V]= {
//      val parameters = scala.io.Source.fromFile("security.csv").mkString.split(",")
//      val t1 = System.nanoTime
//      val advance_sampling = main.sparkContext.parallelize(main.take(rate))
//      val sampling = main.sparkContext.parallelize(main.take(rate))
////      val sampling = main.sparkContext.parallelize(main.take(rate))
//      val duration = (System.nanoTime - t1) / 1e9d
//      if(parameters(2).toInt == 1)// 1 means tune accuracy
//        new dpobjectKV(sampling.map(p => (f(p._1),p._2)), advance_sampling.map(p => (p => f(p._1)), main.subtract(sampling).map(p => (f(p._1),p._2)))
//      else
//        new dpobjectKV(sampling.map(p => (f(p._1),p._2)), advance_sampling.map(p => (p => f(p._1)), main.map(p => (f(p._1),p._2)))
//    }

//  def mapfilter[U: ClassTag](f: String => U, rate: Int): dpfilter[U]= {
//    new dpfilter(main.map(f))
//  }
}
