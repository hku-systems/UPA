package edu.hku.cs.dp

import breeze.linalg.Vector
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD

import scala.math.{exp, pow}
import scala.reflect.ClassTag

/**
  * Created by lionon on 10/28/18.
  */
class dpobjectKV_checker[K, V](var inputoriginal: RDD[((K, V),Long)])
                              (implicit kt: ClassTag[K], vt: ClassTag[V], ord: Ordering[K] = null)
  extends Logging with Serializable
{
  var original = inputoriginal
  val epsilon = 0.1
  val delta = pow(10,-8)
  val k_distance_double = 1/epsilon
  val k_distance = k_distance_double.toInt
  val beta = epsilon / (2*scala.math.log(2/delta))

  def filterDPKV(f: ((K,V)) => Boolean) : dpobjectKV_checker[K, V] = {

    val r3 = inputoriginal.filter(p => f(p._1))

    new dpobjectKV_checker(r3)
  }
  //********************Join****************************
  def joinDP[W](otherDP: RDD[(K, W)]): dpobject_checker[((K, (W, V)))] = {

    val joinresult = original
      .map(p => (p._1._1,(p._1._2,p._2)))
      .join(otherDP)
      .map(p => ((p._1,(p._2._2,p._2._1._1)),p._2._1._2))

    new dpobject_checker(joinresult)
  }

  def joinDP_original[W](otherDP: RDD[(K, W)]): dpobject_checker[((K, (V, W)))] = {

    val joinresult = original
      .map(p => (p._1._1,(p._1._2,p._2)))
      .join(otherDP)
      .map(p => ((p._1,(p._2._1._1,p._2._2)),p._2._1._2))

    new dpobject_checker(joinresult)
  }

}