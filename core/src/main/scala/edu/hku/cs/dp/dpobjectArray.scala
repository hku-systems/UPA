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

  def count(): (Long,Long,Long) = {
    (inputsample.count(),inputsample_advance.count(),inputoriginal.count())
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

  def reduceDP(f: (Double, Double) => Double, k_dist: Int): (Double,Double,Double,Double) = {

    //computin candidates of smooth sensitivity
    var array = reduceDP_deep(f)
    val t1 = System.nanoTime

    val all_samp = array._1.flatMap(p => p) ++ array._2.flatMap(p => p)
    all_samp.foreach(p => println("samp_output: " + p))
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
      (array._3,final_noise,min_bound,max_bound) //sensitivity
    } else {
      val final_noise =  r.nextDouble*diff
      (array._3,final_noise,min_bound,max_bound) //sensitivity
    }
  }

  def reduceDP_deep(f: (Double, Double) => Double) : (Array[Array[Double]],Array[Array[Double]],Double, Double) = {

    val t1 = System.nanoTime
    val parameters = scala.io.Source.fromFile("security.csv").mkString.split(",")
    val epsilon = 1
    val delta = 1
    val k_distance_double = 1/epsilon
    val k_distance = parameters(0).toInt
    val beta = epsilon / (2*scala.math.log(2/delta))

    val s_collect = sample.map(p => (p._2,p._1)).asInstanceOf[RDD[(Long,Double)]].reduceByKey(f).map(_._2).collect()

    //The "sample" field carries the aggregated result already
    var result = 0.0
    if(!original.isEmpty) {
      result = original.asInstanceOf[RDD[Double]].reduce(f)
    }
    val result_b = original.sparkContext.broadcast(result)
    var aggregatedResult = result//get the aggregated result
    //    if(!sample.isEmpty())
    //      aggregatedResult = f(sample.reduce(f),result)//get the aggregated result

    //    val inner = new ArrayBuffer[V]
    var inner_num = 0
    var outer_num = k_distance
    val sample_count = s_collect.length //e.g., 64
    val sample_count_b =  sample.sparkContext.broadcast(s_collect)
    //***********samples*********************
    print("sample count: " + sample_count)
    val sample_array = sample_count match {
      case a if a == 0 =>
        val only_array = new Array[Double](1)
        only_array(0) = aggregatedResult
        Array((0,only_array))
      case b if b == 1 =>
        val only_array = new Array[Double](1)
        only_array(0) = f(result,s_collect.head)
        aggregatedResult = f(result,s_collect.head)
        Array((0,only_array)) //without that sample
      case _ => //more than one sample
        if (sample_count <= k_distance) {
          outer_num = 1 //to make sure all k has a sample point
        } else
          outer_num = k_distance //outer_num = 10
        val i = outer_num //i = 10
        val up_to_index = (sample_count - i).toInt //up_to_index = 8
        val b_i = i // i is the number of layer
        val b_i_b = sample.sparkContext.broadcast(i)
        val n = sample.sparkContext.parallelize((0 to up_to_index - 1).toSeq)// if distance 1, then need 2 differing element here because this layer will not be included into the nieghour array
          .map(p => {
            val upper_array = f(sample_count_b.value.patch(p, Nil, b_i_b.value + 1).reduce(f), result_b.value) //(0 -> 7, 8 -> 15, 16, 24, 32, 40, 48, 56)
            var neighnout_o = new Array[Double](b_i_b.value) //bi is the number of layer
            var j = b_i_b.value - 1 //start form 10, minus one because it is an index
            while (j >= 0) {
              if(j == b_i_b.value - 1)
                neighnout_o(j) = f(upper_array, sample_count_b.value(p + j + 1)) //add back 11, so would be 1 to 10
              else
                neighnout_o(j) = f(sample_count_b.value(p + j + 1), neighnout_o(j + 1))// upper layer, so j+1
              j = j - 1
            }
            (p,neighnout_o)
          }).collect()
        n
        if(!n.isEmpty)
          aggregatedResult = f(n.filter(_._1 == 0).head._2(0),s_collect(0))
        n
    }
    val aggregatedResult_b = sample.sparkContext.broadcast(aggregatedResult)
    val a_collect = sample_advance.map(p => (p._2,p._1)).asInstanceOf[RDD[(Long,Double)]].reduceByKey(f).map(_._2).collect()
    //**********sample advance*************
    val a_collect_b =  sample_advance.sparkContext.broadcast(a_collect)
    val sample_advance_count = a_collect.length
    print("ssample advance count: " + sample_advance_count)
    val sample_array_advance = sample_advance_count match {
      case a if a == 0 =>
        var only_array_advance = new Array[Double](1)
        only_array_advance(0) = aggregatedResult
        Array(only_array_advance)
      case b if b == 1 =>
        var only_array_advance = new Array[Double](1)
        only_array_advance(0) = f(aggregatedResult,a_collect.head)
        Array(only_array_advance) //without that sample
      case _ =>
        if (sample_advance_count <= k_distance)
          outer_num = 1
        else
          outer_num = k_distance //outer_num = 10
        var i = outer_num
        val up_to_index = (sample_advance_count - i).toInt
        val b_i = i // i is the number of layer
        val b_i_b = sample_advance.sparkContext.broadcast(i)
        sample_advance.sparkContext.parallelize((0 to up_to_index - 1).toSeq)
          .map(p => {
            var neighnout_o = new Array[Double](b_i_b.value)
            var j = 0 //start form 10
            while (j < b_i) {
              if(j == 0)
                neighnout_o(j) = f(a_collect(p), aggregatedResult_b.value)
              else
                neighnout_o(j) = f(a_collect(p + j), neighnout_o(j-1))
              j = j + 1
            }
            neighnout_o
          }).collect()
    }
    val duration = (System.nanoTime - t1) / 1e9d
    println("reduce: " + duration)
    (sample_array.map(_._2),sample_array_advance,aggregatedResult,beta)
  }

}