package edu.hku.cs.dp

import breeze.linalg.{DenseVector, Vector}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD

import scala.math.{exp, pow}
import scala.reflect.ClassTag
import scala.util.Random

/**
  * Created by lionon on 10/22/18.
  */
class dpobject_checker[T: ClassTag](
                             var inputoriginal : RDD[(T,Long)])
  extends Logging with Serializable
{
  var original = inputoriginal

  def mapDP[U: ClassTag](f: T => U): dpobject_checker[U]= {
    val with_index = original.map(p => {
      (f(p._1),p._2)
    })
    new dpobject_checker(with_index)
  }

  def isEmptyDP(): Boolean = {
    original.isEmpty()
  }

  def mapDPKV[K: ClassTag,V: ClassTag](f: T => (K,V)): dpobjectKV_checker[K,V]= {
    val r3 = original.map(p => {
      (f(p._1),p._2)
    }).asInstanceOf[RDD[((K,V),Long)]]
    new dpobjectKV_checker(r3)
  }

  def reduceDP(f: (T, T) => T): Unit = {
    val result_by_part = original.map(p => (p._2,p._1)).reduceByKey((a,b) => f(a,b))
    val result_by_part_collect = result_by_part.collect()
    if(result_by_part.isEmpty) {
      println("output is Empty")
    }
    else {
      val final_result = result_by_part.map(p => p._2).reduce(f)
      println("by part: ")
      result_by_part_collect.foreach(println)
      println("final result: " + final_result)
    }
  }

  def reduceDP_vector(f: (T, T) => T): T = {
    val result_by_part = original.map(p => (p._2,p._1)).reduceByKey((a,b) => f(a,b))
    val result_by_part_collect = result_by_part.collect()
    val final_result = result_by_part.map(p => p._2).reduce(f)
    final_result
    }

  def filterDP(f: T => Boolean) : dpobject_checker[T] = {
    val r3 = original.filter(p => f(p._1))
    new dpobject_checker(r3)
  }
}