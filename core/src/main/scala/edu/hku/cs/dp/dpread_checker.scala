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

  def mapDP[U: ClassTag](f: T => U): dpobject_checker[U]= {
    val with_index = main.zipWithIndex().map(p => {
      (f(p._1), p._2 % 10)
    })
    new dpobject_checker(with_index)
  }

}
