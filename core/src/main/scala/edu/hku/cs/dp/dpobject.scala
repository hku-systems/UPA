package edu.hku.cs.dp

import java.util.Random

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.{MapPartitionsRDD, RDD}
import org.apache.spark.util.Utils
import org.apache.spark.util.random.SamplingUtils

import scala.collection.{Map, mutable}
import scala.collection.immutable.HashSet
import scala.reflect.ClassTag
import scala.collection.mutable.ArrayBuffer
import scala.math.{pow,log,exp}

/**
  * Created by lionon on 10/22/18.
  */
class dpobject[T: ClassTag](
  var inputsample : RDD[T],
  var inputsample_advance : RDD[T],
  var inputoriginal : RDD[T])
  extends RDD[T](inputoriginal)
{

  var sample = inputsample //for each element, sample refers to "if this element exists"
  var sample_advance = inputsample_advance
  var original = inputoriginal
  var sample_addition = inputsample
  val epsilon = 0.1
  val delta = pow(10,-8)
  val k_distance_double = 1/epsilon
  val k_distance = k_distance_double.toInt
  val beta = epsilon / (2*scala.math.log(2/delta))


  override def compute(split: org.apache.spark.Partition, context: org.apache.spark.TaskContext): Iterator[T] =
  {
    inputsample.iterator(split, context)
  }

  override protected def getPartitions: Array[org.apache.spark.Partition] =
    inputsample.partitions

  def mapDP[U: ClassTag](f: T => U): dpobject[U]= {
    new dpobject(inputsample.map(f),sample_advance.map(f),inputoriginal.map(f))
  }

  def mapDPKV[K: ClassTag,V: ClassTag](f: T => (K,V)): dpobjectKV[K,V]= {
    new dpobjectKV(inputsample.map(f).asInstanceOf[RDD[(K,V)]],sample_advance.map(f).asInstanceOf[RDD[(K,V)]],inputoriginal.map(f).asInstanceOf[RDD[(K,V)]])
  }

  def reduceDP(f: (T, T) => T) : (Array[RDD[T]],Array[RDD[T]],T) = {
    //The "sample" field carries the aggregated result already

    val result = original.reduce(f)
    val aggregatedResult = f(sample.reduce(f),result)//get the aggregated result
    val broadcast_result = original.sparkContext.broadcast(result)
    val broadcast_aggregatedResult = original.sparkContext.broadcast(aggregatedResult)

    //    val inner = new ArrayBuffer[V]
    var inner_num = 0
    var outer_num = k_distance
    val sample_count = sample.count //e.g., 64
    val sample_advance_count = sample_advance.count
    val broadcast_sample = original.sparkContext.broadcast(sample.collect())
    val broadcast_sample_advance = original.sparkContext.broadcast(sample_advance.collect())
    var array = new Array[RDD[T]](1)
    //***********samples*********************
    if (sample_count <= 1) {
      if(sample_count == 0)
        array(0) = original.sparkContext.parallelize(Seq(aggregatedResult))
      else
        array(0) = original.sparkContext.parallelize(Seq(result)) //without that sample
    }
    else {
      if(sample_count <= k_distance - 1)
        outer_num = k_distance - 1
      else
        outer_num = k_distance //outer_num = 8
      array = new Array[RDD[T]](outer_num)
      var i = outer_num
      while(i  > 0) {
          val up_to_index = (sample_count - i).toInt
        if(i == outer_num) {
//          println("sample_count: " + sample_count)
//          println("outer-most loop: " + up_to_index)

          val inner_array = original.sparkContext.parallelize(0 to up_to_index - 1) //(0,1,2,3,4,5,6,7)
            .map(p => {
            val inside_array = broadcast_sample.value.patch(p, Nil, i)
            f(inside_array.reduce(f),broadcast_result.value) //(0 -> 7, 8 -> 15, 16, 24, 32, 40, 48, 56)
          })
//          println("i is " + i)
          array(i - 1) = inner_array
        } else {
          val array_collected = array(i).collect()
          val upper_array = original.sparkContext.broadcast(array_collected)

          val array_length = array_collected.length
          val array_length_broadcast = original.sparkContext.broadcast(array_length)
          val up_to_index = (sample_count - i).toInt

//          println("sample_count: " + sample_count)
//          println("array_length: " + array_length)
//          println("current i: " + i)
//          println("outer_num: " + outer_num)
//          println("up_to_index: " + up_to_index)

          val inner_array = original.sparkContext.parallelize(0 to up_to_index - 1) //(0,1,2,3,4,5,6,7)
            .map(p => {
            if(p < array_length_broadcast.value) {
              f(upper_array.value(p), broadcast_sample.value(p + i + 1))//no need to include result, as it is included //no need to plus one?
            }
//            else if(p == array_length_broadcast.value)
//              f(upper_array.value(p - 1),broadcast_sample.value(p))//redundant, use upper_array.value(p) twice, means doing the same thing
            else {
              val inside_array = broadcast_sample.value.patch(p, Nil, i)
              f(inside_array.reduce(f),broadcast_result.value)
            }
          })
          array(i - 1) = inner_array
        }
        i = i - 1
      }
    }
    //**********sample advance*************
    var array_advance = new Array[RDD[T]](1)
    if (sample_advance_count <= 1) {
      if(sample_advance_count == 0)
        array_advance(0) = original.sparkContext.parallelize(Seq(aggregatedResult))
      else
        array_advance(0) = original.sparkContext.parallelize(Seq(f(aggregatedResult,sample_advance.collect().head)))
    }
    else {
      if(sample_advance_count <= k_distance - 1)
        outer_num = k_distance - 1
      else
        outer_num = k_distance //outer_num = 8
      array_advance = new Array[RDD[T]](outer_num)
      var i = 0
      while(i  < outer_num) {//hard coded
        if(i == 0) {
          val up_to_index = (sample_advance_count - i).toInt
          val inner_array = original.sparkContext.parallelize(0 to up_to_index - 1) //(0,1,2,3,4,5,6,7)
            .map(p => {
            val inside_array = broadcast_sample_advance.value(p)
            f(inside_array,broadcast_aggregatedResult.value) //(0 -> 7, 8 -> 15, 16, 24, 32, 40, 48, 56)
          })
          array_advance(i) = inner_array
        } else {
          val up_to_index = (sample_advance_count - i).toInt
          val array_collected = array_advance(i - 1).collect()
          val upper_array = original.sparkContext.broadcast(array_collected)
          val b_i = original.sparkContext.broadcast(i)

          val inner_array = original.sparkContext.parallelize(0 to up_to_index - 1) //(0,1,2,3,4,5,6,7)
            .map(p => {
              f(upper_array.value(p), broadcast_sample_advance.value(p + b_i.value))//no need to include result, as it is included
          })
          array_advance(i) = inner_array
        }
        i = i + 1
      }
    }
    (array,array_advance,aggregatedResult)
  }

def reduce_and_add_noise_KDE(f: (T, T) => T): T = {
  //computin candidates of smooth sensitivity

val array = reduceDP(f).asInstanceOf[(Array[RDD[Double]],Array[RDD[Double]],Double)]
  val neigbour_local_senstivity = array._1.map(p => {
    val max = p.max
    val min = p.min
    scala.math.max(scala.math.abs(max - array._3),scala.math.abs(min - array._3))
  })

//  //**********Testing***************
//  println("Verifying correctness")
//  for(i <- 0 until array._1.length)
//  {
//    println("distance is: " + (i+1))
//    var min = (i+1).toDouble
//    val array_collect = array._1(i).sortBy(p => p).collect()
//    for(ii <- 0 until array_collect.length-1)
//      {
//        val diff = array_collect(ii+1) - array_collect(ii)
//        if(diff < min)
//          min = diff
//      }
//    println("min distance is: " + min)
//  }
//
//  println("Verifying correctness")
//  for(i <- 0 until array._2.length)
//  {
//    println("distance is: " + (i+1))
//    var min = (i+1).toDouble
//    val array_collect = array._1(i).sortBy(p => p).collect()
//    for(ii <- 0 until array_collect.length-1)
//    {
//      val diff = array_collect(ii+1) - array_collect(ii)
//      if(diff < min)
//        {
//          println("array_collect(ii+1): " + array_collect(ii+1))
//          println("array_collect(ii): " + array_collect(ii))
//          min = diff
//        }
//    }
//    println("min distance is: " + min)
//  }
//  //**********Testing***************

  val neigbour_local_advance_senstivity = array._2.map(p => {
    val max = p.max
    val min = p.min
    scala.math.max(scala.math.abs(max - array._3),scala.math.abs(min - array._3))
  })

    var max_nls = 0.0
  for (i <- 0 until neigbour_local_senstivity.length) {
    neigbour_local_senstivity(i) = neigbour_local_senstivity(i)*exp(-beta*(i+1))
    if(neigbour_local_senstivity(i) > max_nls)
      max_nls = neigbour_local_senstivity(i)
  }

  for (i <- 0 until neigbour_local_advance_senstivity.length) {
    neigbour_local_advance_senstivity(i) = neigbour_local_advance_senstivity(i)*exp(-beta*(i+1))
    if(neigbour_local_advance_senstivity(i) > max_nls)
      max_nls = neigbour_local_advance_senstivity(i)
  }

  println("sensitivity is: " + max_nls)
  array._3.asInstanceOf[T] //sensitivity
}

  def reduce_and_add_noise_LR(f: (T, T) => T): T = {
    //computin candidates of smooth sensitivity
    val array = reduceDP(f).asInstanceOf[(Array[RDD[Vector[Double]]],Array[RDD[Vector[Double]]],Vector[Double])]
    val vector_length = array._3.length
    val b_result = sample.sparkContext.broadcast(array._3)
    val neigbour_local_senstivity = array._1.map(p => {
      val max = p.reduce((a,b) => {
        a.zip(b).zip(b_result.value).map(q => {
          scala.math.max(scala.math.abs(q._1._1 - q._2),scala.math.abs(q._1._2 - q._2))
        })
      })
      max
    })

    val neigbour_local_advance_senstivity = array._2.map(p => {
      val max = p.reduce((a,b) => {
        a.zip(b).zip(b_result.value).map(q => {
          scala.math.max(scala.math.abs(q._1._1 - q._2),scala.math.abs(q._1._2 - q._2))
        })
      })
      max
    })

    var max_nls = Vector.fill(vector_length)(0.0)
    for (i <- 0 until neigbour_local_senstivity.length) {
//      neigbour_local_senstivity(i) = neigbour_local_senstivity(i)*exp(-beta*(i+1))
      max_nls = max_nls
        .zip(neigbour_local_senstivity(i).asInstanceOf[Vector[Double]])
        .map(p => scala.math.max(p._1,p._2))
    }

    for (i <- 0 until neigbour_local_advance_senstivity.length) {
      //      neigbour_local_senstivity(i) = neigbour_local_senstivity(i)*exp(-beta*(i+1))
      max_nls = max_nls
        .zip(neigbour_local_advance_senstivity(i).asInstanceOf[Vector[Double]])
        .map(p => scala.math.max(p._1,p._2))
    }

    println("sensitivity is: " + max_nls)
    array._2.asInstanceOf[T] //sensitivity
  }

def filterDP(f: T => Boolean) : dpobject[T] = {
  new dpobject(inputsample.filter(f),inputsample_advance.filter(f),inputoriginal.filter(f))
}
}
