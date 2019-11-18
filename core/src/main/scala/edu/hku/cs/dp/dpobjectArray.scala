package edu.hku.cs.dp

import org.apache.spark.rdd.RDD

import scala.math.{exp, pow}
import scala.reflect.ClassTag
import scala.util.Random

/**
  * Created by lionon on 10/22/18.
  */
class dpobjectArray[T: ClassTag](
                                  var inputsample : RDD[(T,Long)],
                                  var inputsample_advance : RDD[(T,Long)],
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
    inputoriginal.iterator(split, context)
  }

  override protected def getPartitions: Array[org.apache.spark.Partition] =
    inputoriginal.partitions

  def mapDP[U: ClassTag](f: T => U): dpobjectArray[U]= {
    val t1 = System.nanoTime
    val r1 = inputsample.map(p => (f(p._1),p._2))
    val r2 = sample_advance.map(p => (f(p._1),p._2))
    val r3 = inputoriginal.map(f)
    val duration = (System.nanoTime - t1) / 1e9d
    println("map: " + duration)
    new dpobjectArray(r1,r2,r3)
  }

  def print_length(): Unit = {
    val original_length = original.count()
    print("dpobjectarray length: " + original_length)
  }

  def mapDPKV[K: ClassTag,V: ClassTag](f: T => (K,V)): dpobjectKVArray[K,V]= {
    val t1 = System.nanoTime
    val r1 = inputsample.map(p => (f(p._1),p._2))
    val r2 = sample_advance.map(p => (f(p._1),p._2))
    val r3 = inputoriginal.map(f)
    val duration = (System.nanoTime - t1) / 1e9d
    println("map: " + duration)
    new dpobjectKVArray(r1,r2,r3)
  }

  def filterDP(f: T => Boolean) : dpobjectArray[T] = {
    val t1 = System.nanoTime
    val r1 = inputsample.filter(p => f(p._1))
    val r2 = inputsample_advance.filter(p => f(p._1))
    val r3 = inputoriginal.filter(f)
    val duration = (System.nanoTime - t1) / 1e9d
    println("filter: " + duration)
    new dpobjectArray(r1,r2,r3)
  }

  def reduceDP(f: (T, T) => T, k_dist: Int): (T,T,T,T) = {

    //computin candidates of smooth sensitivity
    var array = reduceDP_deep(f).asInstanceOf[(Array[Array[Double]],Array[Array[Double]],Double,Double)]
    val t1 = System.nanoTime

    val all_samp = array._1.flatMap(p => p) ++ array._2.flatMap(p => p)
    val sum_all_samp = all_samp.reduce((a,b) => a*a + b*b)
    val r = new Random()
    val original_res = array._3
    var diff = 0.0
    var max_bound = 0.0
    var min_bound = 0.0
    if (!all_samp.isEmpty) {
      val all_samp_normalised_max = all_samp.map(p => p / sum_all_samp + r.nextGaussian() * math.sqrt(sum_all_samp)).zipWithIndex
      val all_samp_noise_max = all_samp_normalised_max.maxBy(_._1)._2
      val max_bound = all_samp(all_samp_noise_max)
      val all_samp_normalised_min = all_samp.map(p => -1 * (p / sum_all_samp + r.nextGaussian() * math.sqrt(sum_all_samp))).zipWithIndex
      val all_samp_noise_min = all_samp_normalised_min.maxBy(_._1)._2
      val min_bound = all_samp(all_samp_noise_min)
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

  def reduceDP_deep(f: (T, T) => T) : (Array[Array[T]],Array[Array[T]],T, Double) = {

    val t1 = System.nanoTime
    val parameters = scala.io.Source.fromFile("security.csv").mkString.split(",")
    val epsilon = 1
    val delta = 1
    val k_distance_double = 1/epsilon
    val k_distance = parameters(0).toInt
    val beta = epsilon / (2*scala.math.log(2/delta))

    val reduce_sample_time = System.nanoTime
    val s_collect = sample.map(p => (p._2,p._1)).reduceByKey(f).map(_._2).take(30)
    val reduce_sample_duration = (System.nanoTime - reduce_sample_time) / 1e9d
    println("reduce_sample_time: " + reduce_sample_duration)

    val reduce_advance_time = System.nanoTime
    val a_collect = sample_advance.map(p => (p._2,p._1)).reduceByKey(f).map(_._2).take(30)
    val reduce_advance_duration = (System.nanoTime - reduce_advance_time) / 1e9d
    println("reduce_advance_time: " + reduce_advance_duration)

    //The "sample" field carries the aggregated result already
    val reduce_original_time = System.nanoTime
    val result = original.reduce(f)
    val reduce_original_duration = (System.nanoTime - reduce_original_time) / 1e9d
    println("reduce_original_time: " + reduce_original_duration)


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
          outer_num = k_distance - 1
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

  def reduce_and_add_noise_KDE(f: (T, T) => T, app_name: String, k_dist: Int): T = {
    //computin candidates of smooth sensitivity

    var array = reduceDP_deep(f).asInstanceOf[(Array[Array[Double]],Array[Array[Double]],Double, Double)]
    val t1 = System.nanoTime
    val beta = array._4
    if(!array._1.isEmpty && !array._2.isEmpty) {
      val stat_sample = array._1
        .map(p => (p, p, p, 1))
        .reduce((a, b) => {
          val max = a._1.zip(b._1).map(x => scala.math.max(x._1, x._2))
          val min = a._2.zip(b._2).map(x => scala.math.min(x._1, x._2))
          val sum = a._3.zip(b._3).map(x => x._1 + x._2)
          (max, min, sum, a._4 + b._4)
        })
      val sample_count = stat_sample._4

      val sample_mean = stat_sample._3.map(s => s / sample_count)

      val sample_variance = array._1.map(s => {
        var sd_inner = s
        for (sa <- 0 until s.length) {
          sd_inner(sa) = pow(s(sa) - sample_mean(sa), 2)
        }
        sd_inner
      }).reduce((a, b) => {
        val zipped = a.zip(b)
        val sum = zipped.map(x => x._1 + x._2)
        sum
      }).map(sv => sv / sample_count)

      val local_sensitivity_sample = stat_sample._1
        .zip(stat_sample._2)
        .map(q => {
          scala.math.max(scala.math.abs(q._1 - array._3), scala.math.abs(q._2 - array._3))
        })

      //********sample advance
      val stat_sample_advance = array._2
        .map(p => (p, p, p, 1))
        .reduce((a, b) => {
          val max = a._1.zip(b._1).map(x => scala.math.max(x._1, x._2))
          val min = a._2.zip(b._2).map(x => scala.math.min(x._1, x._2))
          val sum = a._3.zip(b._3).map(x => x._1 + x._2)
          (max, min, sum, a._4 + b._4)
        })
      val sample_count_advance = stat_sample_advance._4

      val sample_mean_advance = stat_sample_advance._3.map(s => s / sample_count_advance)

      val sample_variance_advance = array._2.map(s => {
        var sd_inner = s
        for (sa <- 0 until s.length) {
          sd_inner(sa) = pow(s(sa) - sample_mean_advance(sa), 2)
        }
        sd_inner
      }).reduce((a, b) => {
        val zipped = a.zip(b)
        val sum = zipped.map(x => x._1 + x._2)
        sum
      }).map(sv => sv / sample_count_advance)

      val local_sensitivity_sample_advance = stat_sample_advance._1
        .zip(stat_sample_advance._2)
        .map(q => {
          scala.math.max(scala.math.abs(q._1 - array._3), scala.math.abs(q._2 - array._3))
        })
      //End of sample advance

      var max_nls = 0.0
      for (i <- 0 until local_sensitivity_sample.length) {
        local_sensitivity_sample(i) = local_sensitivity_sample(i) * exp(-beta * (i + 1))
        if (local_sensitivity_sample(i) > max_nls)
          max_nls = local_sensitivity_sample(i)
      }

      for (i <- 0 until local_sensitivity_sample_advance.length) {
        local_sensitivity_sample_advance(i) = local_sensitivity_sample_advance(i) * exp(-beta * (i + 1))
        if (local_sensitivity_sample_advance(i) > max_nls)
          max_nls = local_sensitivity_sample_advance(i)
      }
    }
    val duration = (System.nanoTime - t1) / 1e9d
    println("calsen: " + duration)
    array._3.asInstanceOf[T] //sensitivity
  }

}