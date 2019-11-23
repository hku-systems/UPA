package edu.hku.cs.dp

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.{MapPartitionsRDD, RDD, RDDOperationScope}

import scala.reflect.ClassTag


/**
  * Created by lionon on 10/22/18.
  */
class dpread_checker[T: ClassTag](
  var rdd1 : RDD[T])
{
  var main = rdd1

  def mapDP[U: ClassTag](f: T => U, rate: Int): dpobject_checker[U]= {
    val b = main.sparkContext.broadcast(rate)
    val with_index = main.zipWithIndex().map(p => {
      (f(p._1), p._2 % b.value)
    })
    new dpobject_checker(with_index)
  }

  def mapDPKV[K: ClassTag,V: ClassTag](f: T => (K,V), rate: Int): dpobjectKV_checker[K,V]= {
    val b = main.sparkContext.broadcast(rate)
    val r3 = main.zipWithIndex().map(p => {
      (f(p._1),p._2 % b.value)
    }).asInstanceOf[RDD[((K,V),Long)]]
    new dpobjectKV_checker(r3)
  }

}
