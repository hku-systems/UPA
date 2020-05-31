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
class dpobject_c[T: ClassTag](
                               var inputsample : RDD[(T,Long)],
                               var inputsample_advance : RDD[T],
                               var inputoriginal : RDD[(T,Long)]) {

  var sample = inputsample //for each element, sample refers to "if this element exists"
  var sample_advance = inputsample_advance
  var original = inputoriginal
  var sample_addition = inputsample


  def mapDP[U: ClassTag](f: T => U): dpobject_c[U] = {
    val t1 = System.nanoTime
    val r1 = inputsample.map(p => (f(p._1), p._2))
    val r2 = sample_advance.map(p => f(p))
    val duration = (System.nanoTime - t1) / 1e9d
    val r3 = inputoriginal.map(p => (f(p._1), p._2))
    new dpobject_c(r1, r2, r3)
  }

  def isEmptyDP(): Boolean = {
    inputoriginal.isEmpty()
  }


  //  def mapDPKV[K: ClassTag,V: ClassTag](f: T => (K,V)): dpobjectKV[K,V]= {
  //
  //    val t1 = System.nanoTime
  //    val r1 = inputsample.map(p => (f(p._1),p._2)).asInstanceOf[RDD[(K,V)]]
  //    val r2 = sample_advance.map(p => (f(p._1),p._2)).asInstanceOf[RDD[(K,V)]]
  //    val r3 = inputoriginal.map(p => (f(p._1),p._2)).asInstanceOf[RDD[(K,V)]]
  //    val duration = (System.nanoTime - t1) / 1e9d
  //    println("map: " + duration)
  //    new dpobjectKV(r1,r2,r3)
  //  }

  def reduceDP_deep_double(f: (Double, Double) => Double): (Array[Array[Double]], Array[Array[Double]], Double, Double) = {
    val t1 = System.nanoTime
    val parameters = scala.io.Source.fromFile("security.csv").mkString.split(',')
    val epsilon = 1
    val delta = 1
    val k_distance_double = 1 / epsilon
    val k_distance = parameters(0).toInt
    //    val beta = epsilon / (2 * scala.math.log(2 / delta))
    var diff_attack = 0
    //The "sample" field carries the aggregated result already
    assert(!original.isEmpty)
    val result = original
      .asInstanceOf[RDD[(Double, Long)]].map(p => (p._2, p._1))
      .reduceByKey(f).collect

    //    print("Original parition size: ")
    //    original.collect.groupBy(_._2).toArray.foreach(p => {
    //      println(p._2.size)
    //    })

    val s_collect_windex = sample
      .asInstanceOf[RDD[(Double, Long)]]
      .collect.groupBy(_._2).toArray

    //    print("Sample parition size: ")
    //    s_collect_windex.foreach(p => {
    //      println(p._2.size)
    //    })

    val agg = s_collect_windex.map(p => { //compute output value with sample item, accumulatively
      val result_val = result.filter(q => q._1 == p._1)
      assert(result_val.size == 1)
      val p2_size = p._2.size
      var acc = new Array[Double](p2_size)
      for (i <- 0 until p2_size) {
        if (i == 0)
          acc(i) = p._2(i)._1
        else
          acc(i) =  f(p._2(i)._1, acc(i - 1))
      }
      assert(result_val.size == 1) //may equal zero (the whole original partition get filtered out), but lets handle this later
      (p._1, acc.map(q => f(q, result_val.head._2)))
    })

    //    val agg = result.map(p => { //compute output value with sample item, accumulatively
    //      val result_val = s_collect_windex.filter(q => q._1 == p._1)
    //      assert(result_val.size == 1)//only partition exists
    //      var array_size = result_val.head._2.size
    //      if(array_size == 0)
    //        array_size = 1
    //      var acc = new Array[Double](array_size)
    //      if(result_val.size > 0) {
    //        assert(result_val.size == 1)
    //        for (i <- 0 until result_val.size) {
    //          if (i == 0)
    //            acc(i) = f(p._2,result_val.head._2(i)._1)
    //          else
    //            acc(i) =  f(acc(i - 1), result_val.head._2(i)._1)
    //
    //        }
    //      }else
    //      {
    //        acc(0) = p._2
    //      }
    //      (p._1, acc)
    //    })

    //agg//accumulative output values of samples
    val sample_collect_size = s_collect_windex.size
    var all_hist = List[(Long, Double)]()
    for (line <- scala.io.Source.fromFile("histoutputs.csv").getLines) { //get each historical output
      val parsed_p = line.split(',') map (q => {
        val s = q.split(':')
        (s(0).trim.toLong, s(1).trim.toDouble)
      })
      all_hist = all_hist ++ parsed_p.toList
    }
    val hist_group = all_hist.groupBy(_._1).toArray
    val comp = agg.map(p => { //see if each partition of sample matched previous output, if yes, then set back for one step and retry
      val p_val = hist_group.filter(q => q._1 == p._1)
      val p2size = p._2.size - 1
      //      assert(p_val.size > 0)
      if (p_val.size > 0) {
        assert(p_val.size == 1) //this is always true as items of the same key only grouped in one group
        var min_i = p2size
        //        print("p._2.size: " + p._2.size)
        var found_diff = 0
        var i = p2size - 1
        val cmp_list = p_val.head._2.map(_._2)
        while (found_diff == 0 && i >= 0) {
          if(i < p2size - 1 && diff_attack ==0)
            diff_attack = 1
          //        for (i <- p._2.size - 1 to 0 by -1) {
          if (!cmp_list.contains(p._2(i))) {//p_val.head._2 is the historical output of the same key, p._2 is the current same acc output
            min_i = i
            found_diff = 1
          }
          i = i - 1
        }
        assert(found_diff == 1)//found a method to change the output of a partition
        (p._1, p._2(min_i),min_i) //the accumulate output of index min_i is chosen
      } else {
        (p._1, p._2.last,p._2.size)//min_i and p._2.size are used for selecting sample below
      }
    })
    var append_str: String = ""
    for (j <- 0 until comp.size) {
      if (j == 0) {
        append_str = comp(j)._1.toString + ":" + comp(j)._2.toString + ","
      } else if (j < comp.size - 1) {
        append_str = append_str + comp(j)._1.toString + ":" + comp(j)._2.toString + ","
      }
      else {
        assert(j == comp.size - 1)
        append_str = append_str + comp(j)._1.toString + ":" + comp(j)._2.toString + "\n"
      }
    }
    scala.tools.nsc.io.File("histoutputs.csv").appendAll(append_str)
    var aggregatedResult = result.map(_._2).reduce(f)
    val result_b = original.sparkContext.broadcast(aggregatedResult)
    //    if(!sample.isEmpty())
    //      aggregatedResult = f(sample.reduce(f),result)//get the aggregated result
    //    val inner = new ArrayBuffer[V]
    var inner_num = 0
    var outer_num = k_distance
    val s_collect = s_collect_windex.map(p => {
      val s = comp.filter(q => p._1 == q._1)
      assert(s.size == 1) //size is 1 because must have index matched
      p._2.take(s.head._3 + 1).map(_._1)
    }).flatten.toArray
    val sample_count = s_collect.length //e.g., 64
    val sample_count_b = sample.sparkContext.broadcast(s_collect)
    //***********samples*********************
    val sample_array = sample_count match {
      case a if a == 0 => //no samlpes
        val only_array = new Array[Double](1) //initialise an array
        only_array(0) = aggregatedResult //put the aggregation result into the array
        Array((0, only_array)) //store the array
      case b if b == 1 => //has only one samples
        val only_array = new Array[Double](1)
        only_array(0) = f(aggregatedResult, s_collect.head)
        Array((0, only_array)) //without that sample
      case _ => //more than one sample
        if (sample_count <= k_distance)
          outer_num = 1 //to make sure all k has a sample point
        else
          outer_num = k_distance //outer_num = 10
        val i = outer_num //i = 10
        val up_to_index = (sample_count - i).toInt //up_to_index = 8
        val b_i = i // i is the number of layer
        val b_i_b = sample.sparkContext.broadcast(i)
        val n = sample.sparkContext.parallelize((0 to up_to_index - 1).toSeq) // if distance 1, then need 2 differing element here because this layer will not be included into the nieghour array
          .map(p => {
            val upper_array = f(sample_count_b.value.patch(p, Nil, b_i_b.value + 1).reduce(f), result_b.value) //(0 -> 7, 8 -> 15, 16, 24, 32, 40, 48, 56)
            var neighnout_o = new Array[Double](b_i_b.value) //bi is the number of layer
            var j = b_i_b.value - 1 //start form 10, minus one because it is an index
            while (j >= 0) {
              if (j == b_i_b.value - 1)
                neighnout_o(j) = f(upper_array, sample_count_b.value(p + j + 1)) //add back 11, so would be 1 to 10
              else
                neighnout_o(j) = f(sample_count_b.value(p + j + 1), neighnout_o(j + 1)) // upper layer, so j+1
              j = j - 1
            }
            (p, neighnout_o)
          }).collect()
        n
        if (!n.isEmpty)
          aggregatedResult = f(n.filter(_._1 == 0).head._2(0), s_collect(0))
        n
    }
    val aggregatedResult_b = sample.sparkContext.broadcast(aggregatedResult)
    //**********sample advance*************
    val a_collect = sample_advance.asInstanceOf[RDD[Double]].collect()
    val a_collect_b = sample_advance.sparkContext.broadcast(a_collect)
    val sample_advance_count = a_collect.length
    val sample_array_advance = sample_advance_count match {
      case a if a == 0 =>
        var only_array_advance = new Array[Double](1)
        only_array_advance(0) = aggregatedResult
        Array(only_array_advance)
      case b if b == 1 =>
        var only_array_advance = new Array[Double](1)
        only_array_advance(0) = f(aggregatedResult, a_collect.head)
        Array(only_array_advance) //without that sample
      case _ =>
        if (sample_advance_count <= k_distance) {
          outer_num = 1
        } else
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
              if (j == 0)
                neighnout_o(j) = f(a_collect(p), aggregatedResult_b.value)
              else
                neighnout_o(j) = f(a_collect(p + j), neighnout_o(j - 1))
              j = j + 1
            }
            neighnout_o
          }).collect()
    }
    val duration = (System.nanoTime - t1) / 1e9d
    if(diff_attack == 1)
      println("Differential attack is detected and avoided")

    (sample_array.map(_._2), sample_array_advance, aggregatedResult, epsilon)
  }

  def reduceDP(f: (Double, Double) => Double): (Double) = {
    //computin candidates of smooth sensitivity
    var array = reduceDP_deep_double(f)
    val t1 = System.nanoTime

    val all_samp = (array._1 ++ array._2).flatten
    val r = new Random()
    var diff = 0.0
    var max_bound = 0.0
    var min_bound = 0.0
    val original_res = array._3
    if (!all_samp.isEmpty) {
      max_bound = all_samp.max
      min_bound = all_samp.min
      diff = (max_bound - min_bound)
    }

    val duration = (System.nanoTime - t1) / 1e9d
    val final_noise = r.nextGaussian() * math.sqrt(diff)
    if (original_res >= min_bound && original_res <= max_bound) {
      original_res + final_noise //sensitivity
    } else {
      val res = min_bound + (max_bound - min_bound) * r.nextDouble()
      res + final_noise //sensitivity
    }
  }

  def filterDP(f: T => Boolean) : dpobject_c[T] = {
    //    val t1 = System.nanoTime
    val r1 = inputsample.filter(p => f(p._1))
    val r2 = inputsample_advance.filter(f)
    val r3 = inputoriginal.filter(p => f(p._1))
    //    val duration = (System.nanoTime - t1) / 1e9d
    new dpobject_c(r1,r2,r3)
  }
}