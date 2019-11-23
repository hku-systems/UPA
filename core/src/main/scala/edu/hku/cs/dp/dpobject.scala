package edu.hku.cs.dp

import breeze.linalg.{DenseVector, Vector}
import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.{MapPartitionsRDD, RDD}
import org.apache.spark.util.Utils
import org.apache.spark.util.random.SamplingUtils
import scala.math.pow
import scala.collection.{Map, mutable}
import scala.collection.immutable.HashSet
import scala.reflect.ClassTag
import scala.collection.mutable.ArrayBuffer
import scala.math.{exp, log, pow}
import scala.util.Random
/**
  * Created by lionon on 10/22/18.
  */
class dpobject[T: ClassTag](
                             var inputsample : RDD[T],
                             var inputsample_advance : RDD[T],
                             var inputoriginal : RDD[T])
{

  var sample = inputsample //for each element, sample refers to "if this element exists"
var sample_advance = inputsample_advance
  var original = inputoriginal
  var sample_addition = inputsample


  def mapDP[U: ClassTag](f: T => U): dpobject[U]= {
        val t1 = System.nanoTime
    val r1 = inputsample.map(f)
    val r2 = sample_advance.map(f)
        val duration = (System.nanoTime - t1) / 1e9d
        println("map: " + duration)
    val r3 = inputoriginal.map(f)
    new dpobject(r1,r2,r3)
  }

  def isEmptyDP(): Boolean = {
    inputoriginal.isEmpty()
  }


  def mapDPKV[K: ClassTag,V: ClassTag](f: T => (K,V)): dpobjectKV[K,V]= {

    val t1 = System.nanoTime
    val r1 = inputsample.map(f).asInstanceOf[RDD[(K,V)]]
    val r2 = sample_advance.map(f).asInstanceOf[RDD[(K,V)]]
    val r3 = inputoriginal.map(f).asInstanceOf[RDD[(K,V)]]
    val duration = (System.nanoTime - t1) / 1e9d
    println("map: " + duration)
    new dpobjectKV(r1,r2,r3)
  }

  def reduceDP_deep(f: (T, T) => T) : (Array[Array[T]],Array[Array[T]],T, Double) = {

    val t1 = System.nanoTime
    val parameters = scala.io.Source.fromFile("security.csv").mkString.split(",")
    val epsilon = 1
    val delta = 1
    val k_distance_double = 1/epsilon
    val k_distance = parameters(0).toInt
    val beta = epsilon / (2*scala.math.log(2/delta))

    val s_collect = sample.take(30)
    val a_collect = sample_advance.take(30)

    //The "sample" field carries the aggregated result already
    val result = original.reduce(f)
    var aggregatedResult = result//get the aggregated result
//    if(!sample.isEmpty())
//      aggregatedResult = f(sample.reduce(f),result)//get the aggregated result

    //    val inner = new ArrayBuffer[V]
    var inner_num = 0
    var outer_num = k_distance
    val sample_count = s_collect.length //e.g., 64
    val sample_advance_count = s_collect.length
    //***********samples*********************

    val sample_array = sample_count match {
      case a if a == 0 =>
        val only_array = new Array[T](1)
        only_array(0) = aggregatedResult
        Array(only_array)
      case b if b == 1 =>
        val only_array = new Array[T](1)
        only_array(0) = f(result,s_collect.head)
        Array(only_array) //without that sample
      case _ =>
        if (sample_count <= k_distance)
          outer_num = k_distance - 1 //to make sure all k has a sample point
        else
          outer_num = k_distance //outer_num = 10
      var i = outer_num + 1//11
      val up_to_index = (sample_count - i).toInt
        val b_i = i
        val inner_array = (0 to up_to_index - 1).toArray
          .map(p => {
            f(s_collect.patch(p, Nil, b_i).reduce(f), result) //(0 -> 7, 8 -> 15, 16, 24, 32, 40, 48, 56)
          })
        val upper_array = inner_array
        val n = (0 to up_to_index - 1).toArray //(0,1,2,3,4,5,6,7)
          .map(p => {
          var neighnout_o = new Array[T](b_i - 1)
          var j = b_i - 1 //start form 10
          while (j >= 1) {
            if(j == b_i - 1)
              neighnout_o(j - 1) = f(upper_array(p), s_collect(p + j + 1)) //add back 11, so would be 1 to 10
            else
              neighnout_o(j - 1) = f(s_collect(p + j + 1), neighnout_o(j))
            j = j - 1
          }
          neighnout_o
        })

        if(!n.isEmpty && !n.head.isEmpty)
        aggregatedResult = f(n.head.head,s_collect.head)
        n
    }

    //**********sample advance*************

    val sample_array_advance = sample_advance_count match {
      case a if a == 0 =>
        var only_array_advance = new Array[T](1)
        only_array_advance(0) = aggregatedResult
        Array(only_array_advance)
      case b if b == 1 =>
        var only_array_advance = new Array[T](1)
        only_array_advance(0) = f(aggregatedResult,a_collect.head)
        Array(only_array_advance) //without that sample
      case _ =>
        if (sample_advance_count <= k_distance)
          outer_num = k_distance - 1
        else
          outer_num = k_distance //outer_num = 10
      var i = outer_num
        val up_to_index = (sample_advance_count - i).toInt
        val b_i = i
        (0 to up_to_index - 1).toArray
          .map(p => {
            var neighnout_o = new Array[T](b_i )
            var j = 0 //start form 10
            while (j < b_i) {
              if(j == 0)
                neighnout_o(j) = f(a_collect(p), aggregatedResult)
              else
                neighnout_o(j) = f(a_collect(p + j), neighnout_o(j-1))
              j = j + 1
            }
            neighnout_o
          })
    }
    val duration = (System.nanoTime - t1) / 1e9d
    println("reduce: " + duration)
    (sample_array,sample_array_advance,aggregatedResult,beta)
  }

  def reduceDP(f: (T, T) => T, k_dist: Int): (T,T,T,T) = {

    //computin candidates of smooth sensitivity
    var array = reduceDP_deep(f).asInstanceOf[(Array[Array[Double]],Array[Array[Double]],Double,Double)]
    val t1 = System.nanoTime

    val all_samp = array._1.flatMap(p => p) ++ array._2.flatMap(p => p)
    val r = new Random()
    var diff = 0.0
    var max_bound = 0.0
    var min_bound = 0.0
    val original_res = array._3
    if (!all_samp.isEmpty) {
      max_bound = all_samp.max
      min_bound = all_samp.min
      diff = max_bound - min_bound
    }

    val duration = (System.nanoTime - t1) / 1e9d
    println("calsen: " + duration)

    if(original_res >= min_bound && original_res <= max_bound) {
      val final_noise =  r.nextGaussian()*math.sqrt(diff)
      (array._3,final_noise,min_bound,max_bound).asInstanceOf[(T,T,T,T)] //sensitivity
    } else {
      val final_noise =  r.nextDouble*diff
      (array._3,final_noise,min_bound,max_bound).asInstanceOf[(T,T,T,T)] //sensitivity
    }
  }

  def reduce_and_add_noise_LR(f: (T, T) => T, app_name: String, k_dist: Int): T = {
    //computin candidates of smooth sensitivity

    val array = reduceDP_deep(f).asInstanceOf[(Array[Array[Vector[Double]]], Array[Array[Vector[Double]]], Vector[Double], Double)]
    val t1 = System.nanoTime
    val beta = array._4
    if(!array._1.isEmpty && !array._2.isEmpty) {
      val stat_sample = array._1
        .map(p => (p, p, p, 1))
        .reduce((a, b) => {

          val max = a._1.zip(b._1).map(x => {
            Vector(x._1.toArray.zip(x._2.toArray).map(qm => scala.math.max(qm._1, qm._2)))
          })

          val min = a._2.zip(b._2).map(x => {
            Vector(x._1.toArray.zip(x._2.toArray).map(qm => scala.math.min(qm._1, qm._2)))
          })

          val sum = a._3.zip(b._3).map(x => {
            Vector(x._1.toArray.zip(x._2.toArray).map(qm => qm._1 + qm._2))
          })

          (max, min, sum, a._4 + b._4)
        })

      val sample_count = stat_sample._4
      val sample_mean = stat_sample._3.map(s => s.map(ss => ss / sample_count))
      val sample_variance = array._1.map(s => {

        s.zipWithIndex.map(ss => Vector(ss._1.toArray.zipWithIndex.map(sss => pow(sss._1 - sample_mean(ss._2)(sss._2), 2))))

      }).reduce((a, b) => {
        val zipped = a.zip(b)
        val sum = zipped.map(x => Vector(x._1.toArray.zip(x._2.toArray).map(qm => qm._1 + qm._2)))
        sum
      }).map(sv => sv.map(svv => svv / sample_count))

      val local_sensitivity_sample = stat_sample._1
        .zip(stat_sample._2)
        .map(q => {
          DenseVector(q._1.toArray.zip(q._2.toArray).zipWithIndex
            .map(qm => scala.math.max(scala.math.abs(qm._1._1 - array._3(qm._2)), scala.math.abs(qm._1._2 - array._3(qm._2)))))
        })

      //****************sample advance

      val stat_sample_advance = array._2
        .map(p => (p, p, p, 1))
        .reduce((a, b) => {

          val max = a._1.zip(b._1).map(x => {
            Vector(x._1.toArray.zip(x._2.toArray).map(qm => scala.math.max(qm._1, qm._2)))
          })

          val min = a._2.zip(b._2).map(x => {
            Vector(x._1.toArray.zip(x._2.toArray).map(qm => scala.math.min(qm._1, qm._2)))
          })

          val sum = a._3.zip(b._3).map(x => {
            Vector(x._1.toArray.zip(x._2.toArray).map(qm => qm._1 + qm._2))
          })

          (max, min, sum, a._4 + b._4)
        })

      val sample_count_advance = stat_sample_advance._4
      val sample_mean_advance = stat_sample_advance._3.map(s => s.map(ss => ss / sample_count_advance))
      val sample_variance_advance = array._2.map(s => {

        s.zipWithIndex.map(ss => Vector(ss._1.toArray.zipWithIndex.map(sss => pow(sss._1 - sample_mean_advance(ss._2)(sss._2), 2))))

      }).reduce((a, b) => {
        val zipped = a.zip(b)
        val sum = zipped.map(x => Vector(x._1.toArray.zip(x._2.toArray).map(qm => qm._1 + qm._2)))
        sum
      }).map(sv => sv.map(svv => svv / sample_count_advance))

      val local_sensitivity_sample_advance = stat_sample_advance._1
        .zip(stat_sample_advance._2)
        .map(q => {
          DenseVector(q._1.toArray.zip(q._2.toArray).zipWithIndex.map(qm => scala.math.max(scala.math.abs(qm._1._1 - array._3(qm._2)), scala.math.abs(qm._1._2 - array._3(qm._2)))))
        })

      for (i <- 0 until local_sensitivity_sample.length) {
        local_sensitivity_sample(i) = local_sensitivity_sample(i).map(p => p * exp(-beta * (i + 1)))
      }

      for (i <- 0 until local_sensitivity_sample_advance.length) {
        local_sensitivity_sample_advance(i) = local_sensitivity_sample_advance(i).map(p => p * exp(-beta * (i + 1)))
      }
      val final_sensitivity = (local_sensitivity_sample ++ local_sensitivity_sample_advance).reduce((a, b) => {
        val v_length = a.length
        val new_v = new Array[Double](v_length)
        for (v2 <- 0 until v_length) {
          new_v(v2) = scala.math.max(a(v2), b(v2))
        }
        DenseVector(new_v)
      })
    }
    val duration = (System.nanoTime - t1) / 1e9d
    println("calsen: " + duration)
    array._3.asInstanceOf[T]
  }

  def reduce_and_add_noise_KM(f: (T, T) => T, app_name: String, k_dist: Int): Vector[Double] = {
    //computin candidates of smooth sensitivity

    val array = reduceDP_deep(f).asInstanceOf[(Array[Array[(Vector[Double],Int)]], Array[Array[(Vector[Double],Int)]], (Vector[Double],Int), Double)]
    val t1 = System.nanoTime
    val beta = array._4
    val return_result = array._3._1.map(p => p/array._3._2)

    if(!array._1.isEmpty && !array._2.isEmpty) {
      val sample_n = array._1.map(q => q.map(v => v._1.map(v1 => v1 / v._2)))
      val sample_a = array._2.map(q => q.map(v => v._1.map(v1 => v1 / v._2)))
      val stat_sample = sample_n
        .map(p => (p, p, p, 1))
        .reduce((a, b) => {

          val max = a._1.zip(b._1).map(x => {
            Vector(x._1.toArray.zip(x._2.toArray).map(qm => scala.math.max(qm._1, qm._2)))
          })

          val min = a._2.zip(b._2).map(x => {
            Vector(x._1.toArray.zip(x._2.toArray).map(qm => scala.math.min(qm._1, qm._2)))
          })

          val sum = a._3.zip(b._3).map(x => {
            Vector(x._1.toArray.zip(x._2.toArray).map(qm => qm._1 + qm._2))
          })

          (max, min, sum, a._4 + b._4)
        })

      val sample_count = stat_sample._4
      val sample_mean = stat_sample._3.map(s => s.map(ss => ss / sample_count))
      val sample_variance = sample_n.map(s => {

        s.zipWithIndex.map(ss => Vector(ss._1.toArray.zipWithIndex.map(sss => pow(sss._1 - sample_mean(ss._2)(sss._2), 2))))

      }).reduce((a, b) => {
        val zipped = a.zip(b)
        val sum = zipped.map(x => Vector(x._1.toArray.zip(x._2.toArray).map(qm => qm._1 + qm._2)))
        sum
      }).map(sv => sv.map(svv => svv / sample_count))

      val local_sensitivity_sample = stat_sample._1
        .zip(stat_sample._2)
        .map(q => {
          DenseVector(q._1.toArray.zip(q._2.toArray).zipWithIndex
            .map(qm => scala.math.max(scala.math.abs(qm._1._1 - return_result(qm._2)), scala.math.abs(qm._1._2 - return_result(qm._2)))))
        })

      //****************sample advance

      val stat_sample_advance = sample_a
        .map(p => (p, p, p, 1))
        .reduce((a, b) => {

          val max = a._1.zip(b._1).map(x => {
            Vector(x._1.toArray.zip(x._2.toArray).map(qm => scala.math.max(qm._1, qm._2)))
          })

          val min = a._2.zip(b._2).map(x => {
            Vector(x._1.toArray.zip(x._2.toArray).map(qm => scala.math.min(qm._1, qm._2)))
          })

          val sum = a._3.zip(b._3).map(x => {
            Vector(x._1.toArray.zip(x._2.toArray).map(qm => qm._1 + qm._2))
          })

          (max, min, sum, a._4 + b._4)
        })

      val sample_count_advance = stat_sample_advance._4
      val sample_mean_advance = stat_sample_advance._3.map(s => s.map(ss => ss / sample_count_advance))
      val sample_variance_advance = sample_a.map(s => {

        s.zipWithIndex.map(ss => Vector(ss._1.toArray.zipWithIndex.map(sss => pow(sss._1 - sample_mean_advance(ss._2)(sss._2), 2))))

      }).reduce((a, b) => {
        val zipped = a.zip(b)
        val sum = zipped.map(x => Vector(x._1.toArray.zip(x._2.toArray).map(qm => qm._1 + qm._2)))
        sum
      }).map(sv => sv.map(svv => svv / sample_count_advance))

      val local_sensitivity_sample_advance = stat_sample_advance._1
        .zip(stat_sample_advance._2)
        .map(q => {
          DenseVector(q._1.toArray.zip(q._2.toArray).zipWithIndex.map(qm => scala.math.max(scala.math.abs(qm._1._1 - return_result(qm._2)), scala.math.abs(qm._1._2 - return_result(qm._2)))))
        })

      for (i <- 0 until local_sensitivity_sample.length) {
        local_sensitivity_sample(i) = local_sensitivity_sample(i).map(p => p * exp(-beta * (i + 1)))
      }

      for (i <- 0 until local_sensitivity_sample_advance.length) {
        local_sensitivity_sample_advance(i) = local_sensitivity_sample_advance(i).map(p => p * exp(-beta * (i + 1)))
      }
      val final_sensitivity = (local_sensitivity_sample ++ local_sensitivity_sample_advance).reduce((a, b) => {
        val v_length = a.length
        val new_v = new Array[Double](v_length)
        for (v2 <- 0 until v_length) {
          new_v(v2) = scala.math.max(a(v2), b(v2))
        }
        DenseVector(new_v)
      })
    }
    val duration = (System.nanoTime - t1) / 1e9d
    println("calsen: " + duration)
    return_result
  }

  def filterDP(f: T => Boolean) : dpobject[T] = {
    val t1 = System.nanoTime
    val r1 = inputsample.filter(f)
    val r2 = inputsample_advance.filter(f)
    val r3 = inputoriginal.filter(f)
    val duration = (System.nanoTime - t1) / 1e9d
    println("sample: " + duration)
    new dpobject(r1,r2,r3)
  }
}