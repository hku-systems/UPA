package edu.hku.cs.dp

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import breeze.linalg.{DenseVector, Vector}
import scala.collection.immutable.HashMap
import scala.collection.immutable.HashSet
import scala.collection.mutable.ArrayBuffer
import scala.math.{exp, pow}
import scala.reflect.ClassTag

/**
  * Created by lionon on 10/28/18.
  */
class dpobjectKV[K, V](var inputsample: RDD[(K, V)], var inputsample_advance: RDD[(K, V)], var inputoriginal: RDD[(K, V)])
                      (implicit kt: ClassTag[K], vt: ClassTag[V], ord: Ordering[K] = null)
  extends Logging with Serializable
{
  var sample = inputsample
  var sample_advance = inputsample_advance
  var original = inputoriginal
  val epsilon = 0.1
  val delta = pow(10,-8)
  val k_distance_double = 1/epsilon
  val k_distance = k_distance_double.toInt
  val beta = epsilon / (2*scala.math.log(2/delta))


  //parallelise inter key operation or intra key operation
  //seems use less collect is better
  //because collect then parallelise requires more rtt and sorting
  def reduceByKeyDP_deep(func: (V, V) => V): (Array[(K,Array[RDD[V]])],Array[(K,Array[RDD[V]])],RDD[(K,V)]) = {
    //reduce shuffle, use map rather than reduce/reduceByKey
    val t1 = System.nanoTime
    val originalresult = inputoriginal.reduceByKey(func)//Reduce original first
    val aggregatedResult = originalresult.union(sample).reduceByKey(func)
    val broadcast_result = original.sparkContext.broadcast(originalresult.collect().toMap)


    val nighnouring_output = originalresult.join(sample).map(p => (p._1,p._2._2)).groupByKey().collect().map(p => {
      var inner_num = 0
      var outer_num = k_distance
      val sample_count = p._2.size //e.g., 64
      //            println("sample count: " + sample_count)
      val broadcast_sample = original.sparkContext.broadcast(p._2.toArray)
      if (sample_count <= 1) {
        val inner = new Array[RDD[V]](1) //directly return
        if(sample_count == 0)
          inner(0) = original.sparkContext.parallelize(aggregatedResult.lookup(p._1))
        else
          inner(0) = original.sparkContext.parallelize(originalresult.lookup(p._1)) //without that sample
        (p._1,inner)
      }
      else {
        if(sample_count <= k_distance - 1)
          outer_num = k_distance - 1
        else
          outer_num = k_distance //outer_num = 8
        var array = new Array[RDD[V]](outer_num)
        var i = outer_num
        while(i  > 0) {
          val up_to_index = (sample_count - i).toInt
          if(i == outer_num) {
            val b_i = original.sparkContext.broadcast(i)
            //          println("sample_count: " + sample_count)
            //          println("outer-most loop: " + up_to_index)
            val inner_array = original.sparkContext.parallelize(0 to up_to_index - 1) //(0,1,2,3,4,5,6,7)
              .map(q => {
              val inside_array = broadcast_sample.value.patch(q, Nil, b_i.value)
              func(inside_array.reduce(func),Seq(broadcast_result.value.get(p._1)).flatMap(l => l).head)
            })
            //                  inner_array.collect().foreach(println)
            array(i - 1) = inner_array
          } else {
            val b_i = original.sparkContext.broadcast(i)
            val array_collected = array(i).collect()
            val upper_array = original.sparkContext.broadcast(array_collected)

            val array_length = array_collected.length
            val array_length_broadcast = original.sparkContext.broadcast(array_length)
            val up_to_index = (sample_count - i).toInt

            val inner_array = original.sparkContext.parallelize(0 to up_to_index - 1) //(0,1,2,3,4,5,6,7)
              .map(q => {
              if(q < array_length_broadcast.value) {
                func(upper_array.value(q), broadcast_sample.value(q + b_i.value + 1))//no need to include result, as it is included
              }
              else {
                val inside_array = broadcast_sample.value.patch(q, Nil, b_i.value)
                func(inside_array.reduce(func),Seq(broadcast_result.value.get(p._1)).flatMap(l => l).head)
              }
            })
            array(i - 1) = inner_array
          }
          i = i - 1
        }
        (p._1,array)
      }
    })

    //******************Sample advance************
    val nighnouring_advance_output = aggregatedResult.join(sample_advance).map(p => (p._1,p._2._2)).groupByKey().collect().map(p => {
      var inner_num = 0
      val aggresult_key = aggregatedResult.lookup(p._1)
      val b_aggresult_key = sample_advance.sparkContext.broadcast(aggresult_key)
      var outer_num = k_distance
      val sample_inner = sample_advance.sparkContext.broadcast(p._2.toSeq)
      val sample_inner_count = p._2.size //e.g., 64
      val broadcast_sample = original.sparkContext.broadcast(p._2.toArray)
      if (sample_inner_count <= 1) {
        val inner = new Array[RDD[V]](1) //directly return
        if(sample_inner_count == 0)
          inner(0) = original.sparkContext.parallelize(aggresult_key)
        else
          inner(0) = original.sparkContext.parallelize(Seq(func(aggresult_key.head,p._2.head))) //without that sample
        (p._1,inner)
      }
      else {
        if(sample_inner_count <= k_distance - 1)
          outer_num = k_distance - 1
        else
          outer_num = k_distance //outer_num = 8
        var array_advance = new Array[RDD[V]](outer_num)
        var i = 0
        while(i  < outer_num) {
          if(i == 0) {
            val up_to_index = (sample_inner_count - i).toInt
            val inner_array = original.sparkContext.parallelize(0 to up_to_index - 1) //(0,1,2,3,4,5,6,7)
              .map(p => {
              func(sample_inner.value(p),b_aggresult_key.value.head) //(0 -> 7, 8 -> 15, 16, 24, 32, 40, 48, 56)
            })                //          println("sample_count: " + sample_count)
            //          println("outer-most loop: " + up_to_index)
            array_advance(i) = inner_array
          } else {
            val up_to_index = (sample_inner_count - i).toInt
            val array_collected = array_advance(i - 1).collect()
            val upper_array = original.sparkContext.broadcast(array_collected)
            val b_i = original.sparkContext.broadcast(i)

            val inner_array = original.sparkContext.parallelize(0 to up_to_index - 1) //(0,1,2,3,4,5,6,7)
              .map(p => {
              func(upper_array.value(p), sample_inner.value(p + b_i.value))//no need to include result, as it is included
            })
            //                inner_array.collect().foreach(println)
            array_advance(i) = inner_array
          }
          i = i + 1
        }
        (p._1,array_advance)
      }
    })
    val duration = (System.nanoTime - t1) / 1e9d
    println("reducebykey: " + duration)
    (nighnouring_output,nighnouring_advance_output,aggregatedResult)
  }

  def reduceByKeyDP_Int(func: (V, V) => V, app_name: String, k_dist: Int): RDD[(K,V)] = {

    val array = reduceByKeyDP_deep(func)
    var meta_minus_outer = new Array[(K,Array[String])](array._1.length)
    var minus_outer_count = 0
    val ls = array._1.map(p => {//each key
      val value_of_key = array._3.asInstanceOf[RDD[(K,Int)]].lookup(p._1)(0)
      var a1_length = p._2.length
      var meta_minus_inner = new Array[String](a1_length)
      var neigbour_local_senstivity = new Array[Double](a1_length)
      for(a1 <- 0 until a1_length)
      {
        val q = p._2(a1).asInstanceOf[RDD[Int]]
        if(!q.isEmpty) {
          val max = q.max
          val min = q.min
          val mean = q.mean
          val sd = q.stdev
          val counting = q.count()
          meta_minus_inner(a1) = app_name + "," + k_dist + "," + ((a1 + 1) * (-1)) + "," + mean + "," + sd + "," + counting
          neigbour_local_senstivity(a1) = scala.math.max(scala.math.abs(max - value_of_key), scala.math.abs(min - value_of_key))
        }
        else
        {
          meta_minus_inner(a1) = app_name + "," + k_dist + "," + ((a1 + 1) * (-1)) + "," + value_of_key + "," + 0 + "," + 0
          neigbour_local_senstivity(a1) = 0
        }
      }
      meta_minus_outer(minus_outer_count) = (p._1, meta_minus_inner)
      minus_outer_count = minus_outer_count + 1

      var max_nls = 0.0
      for (i <- 0 until neigbour_local_senstivity.length) {
        neigbour_local_senstivity(i) = neigbour_local_senstivity(i)*exp(-beta*(i+1))
        if(neigbour_local_senstivity(i) > max_nls)
          max_nls = neigbour_local_senstivity(i)
      }
      (p._1,max_nls)
    })

    var meta_positive_outer = new Array[(K,Array[String])](array._2.length)
    var positive_outer_count = 0
    val ls_advance = array._2.map(p => {//each key
      val value_of_key = array._3.asInstanceOf[RDD[(K,Int)]].lookup(p._1)(0)
      val a1_length = p._2.length
      var meta_positive_inner = new Array[String](a1_length)
      var neigbour_local_senstivity = new Array[Double](a1_length)
      for(a1 <- 0 until a1_length)
      {
        val q = p._2(a1).asInstanceOf[RDD[Int]]
        if(!q.isEmpty) {
          val max = q.max
          val min = q.min
          val mean = q.mean
          val sd = q.stdev
          val counting = q.count()
          meta_positive_inner(a1) = app_name + "," + k_dist + "," + (a1 + 1) + "," + mean + "," + sd + "," + counting
          neigbour_local_senstivity(a1) = scala.math.max(scala.math.abs(max - value_of_key), scala.math.abs(min - value_of_key))
        }
        else
        {
          meta_positive_inner(a1) = app_name + "," + k_dist + "," + (a1 + 1) + "," + value_of_key + "," + 0 + "," + 0
          neigbour_local_senstivity(a1) = 0
        }
      }
      meta_positive_outer(positive_outer_count) = (p._1, meta_positive_inner)
      positive_outer_count = positive_outer_count + 1

      var max_nls = 0.0
      for (i <- 0 until neigbour_local_senstivity.length) {
        neigbour_local_senstivity(i) = neigbour_local_senstivity(i)*exp(-beta*(i+1))
        if(neigbour_local_senstivity(i) > max_nls)
          max_nls = neigbour_local_senstivity(i)
      }
      (p._1,max_nls)
    })

    val all_meta = (meta_minus_outer ++ meta_positive_outer).groupBy(_._1).map(p => {
      p._2.foreach(q => {
        q._2.foreach(println)
      })
    })

    val sensitivity_array = ls ++ ls_advance
    val sensitivity = array._2.head._2.head.sparkContext.parallelize(sensitivity_array)
      .reduceByKey((a,b) => scala.math.max(a,b))

    array._3
  }

  def reduceByKeyDP_KM(func: (V, V) => V, app_name: String, k_dist: Int): RDD[(K,V)] = {

    val array = reduceByKeyDP_deep(func)

    val entire = (array._1 ++ array._2).groupBy(_._1)
    val pairing = entire.map(q => {//for each k
      println("How many array are there: " + q._2.length)
      q._2.foreach(qqq => qqq._2.length)
      val sample_l = q._2(0)._2.asInstanceOf[Array[RDD[(Vector[Double],Int)]]]//distance array of each k
      val sample_a = q._2(1)._2.asInstanceOf[Array[RDD[(Vector[Double],Int)]]]
      val result = array._3.asInstanceOf[RDD[(K,(Vector[Double],Int))]]
      val a1_length = sample_l.length
      val a2_length = sample_a.length
      val result_length = result.take(1).head._2._1.length
      println("a1_length is : " + a1_length)
      println("a2_length is : " + a2_length)

      var neigbour_local_senstivity = new Array[Array[Double]](a1_length)
      var neigbour_local_advance_senstivity = new Array[Array[Double]](a2_length)

      for(vc <- 0 until a1_length){
        neigbour_local_senstivity(vc) = new Array[Double](result_length)
      }

      for(vca <- 0 until a2_length) {
        neigbour_local_advance_senstivity(vca) = new Array[Double](result_length)
      }

      for (v1 <- 0 until result_length) {
        val original_vector = array._3.asInstanceOf[RDD[(K,(Vector[Double],Int))]]
        val value_of_key = original_vector.lookup(q._1)(0)._1
        val b_v1 = original.sparkContext.broadcast(v1)
        for (a1 <- 0 until a1_length) {
          val p = sample_l(a1).map(x => x._1(b_v1.value)/x._2)
          if(!p.isEmpty()) {
            val max = p.max
            val min = p.min
            val mean = p.mean
            val sd = p.stdev
            val counting = p.count()
            println(app_name + "," + k_dist + "," + ((a1 + 1) * (-1)) + "," + mean + "," + sd + "," + counting)
            neigbour_local_senstivity(a1)(v1) = scala.math.max(scala.math.abs(max - value_of_key(v1)), scala.math.abs(min - value_of_key(v1)))
          }
          else {
            println(app_name + "," + k_dist + "," + ((a1 + 1) * (-1)) + "," + 0.0 + "," + 0.0 + "," + 0)
            neigbour_local_senstivity(a1)(v1) = 0.0
          }
        }

        for(a2 <- 0 until a2_length)
        {
          val p = sample_a(a2).map(x => x._1(b_v1.value)/x._2)
          if(!p.isEmpty()) {
            val max = p.max
            val min = p.min
            val mean = p.mean
            val sd = p.stdev
            val counting = p.count()
            println(app_name + "," + k_dist + "," + (a2 + 1) + "," + mean + "," + sd + "," + counting)
            neigbour_local_advance_senstivity(a2)(v1) = scala.math.max(scala.math.abs(max - value_of_key(v1)), scala.math.abs(min - value_of_key(v1)))
          }
          else {
            println(app_name + "," + k_dist + "," + (a2 + 1) + "," + 0.0 + "," + 0.0 + "," + 0)
            neigbour_local_advance_senstivity(a2)(v1) = 0.0
          }
        }
      }

      for (i <- 0 until neigbour_local_senstivity.length) {
        neigbour_local_senstivity(i) = neigbour_local_senstivity(i).map(p => p*exp(-beta*(i+1)))
      }

      for (i <- 0 until neigbour_local_advance_senstivity.length) {
        neigbour_local_advance_senstivity(i) = neigbour_local_advance_senstivity(i).map(p => p*exp(-beta*(i+1)))
      }
      val final_sensitivity = (neigbour_local_senstivity ++ neigbour_local_advance_senstivity).reduce((a,b) => {
        val v_length = a.length
        val new_v = new Array[Double](v_length)
        for(v2 <- 0 until v_length)
        {
          new_v(v2) = scala.math.max(a(v2),b(v2))
        }
        new_v
      })
      (q._1,final_sensitivity)
      println("Sensitivity key is: " + q._1 + " and final sensitivity is: " + final_sensitivity)
    })
    array._3
  }

  def reduceByKeyDP_Double(func: (V, V) => V, app_name: String, k_dist: Int): RDD[(K,V)] = {

    val array = reduceByKeyDP_deep(func)
    val entire = (array._1 ++ array._2).groupBy(_._1)

    val pairing = entire.map(q => { //for each k
      val sample_l = q._2(0)._2.asInstanceOf[Array[RDD[Double]]] //distance array of each k
    val sample_a = q._2(1)._2.asInstanceOf[Array[RDD[Double]]]
      val result = array._3.asInstanceOf[RDD[(K, Double)]]
      val a1_length = sample_l.length
      val a2_length = sample_a.length

      val original_vector = array._3.asInstanceOf[RDD[(K,Double)]]
      val value_of_key = original_vector.lookup(q._1)(0)
      var neigbour_local_senstivity = new Array[Double](a1_length)
      for (a1 <- 0 until a1_length) {
        val p = sample_l(a1)
        val max = p.max
        val min = p.min
        val mean = p.mean
        val sd = p.stdev
        val counting = p.count()
        println(app_name + "," + k_dist + "," + ((a1 + 1) * (-1)) + "," + mean + "," + sd + "," + counting)
        neigbour_local_senstivity(a1) = scala.math.max(scala.math.abs(max - value_of_key), scala.math.abs(min - value_of_key))
      }

      var neigbour_local_advance_senstivity = new Array[Double](a2_length)
      for (a2 <- 0 until a2_length) {
        val p = sample_a(a2)
        val max = p.max
        val min = p.min
        val mean = p.mean
        val sd = p.stdev
        val counting = p.count()
        println(app_name + "," + k_dist + "," + (a2 + 1) + "," + mean + "," + sd + "," + counting)
        neigbour_local_advance_senstivity(a2) = scala.math.max(scala.math.abs(max - value_of_key), scala.math.abs(min - value_of_key))
      }

      var max_nls = 0.0
      for (i <- 0 until neigbour_local_senstivity.length) {
        neigbour_local_senstivity(i) = neigbour_local_senstivity(i) * exp(-beta * (i + 1))
        if (neigbour_local_senstivity(i) > max_nls)
          max_nls = neigbour_local_senstivity(i)
      }

      for (i <- 0 until neigbour_local_advance_senstivity.length) {
        neigbour_local_advance_senstivity(i) = neigbour_local_advance_senstivity(i) * exp(-beta * (i + 1))
        if (neigbour_local_advance_senstivity(i) > max_nls)
          max_nls = neigbour_local_advance_senstivity(i)
      }
      (q._1,max_nls)
    })

    array._3
  }

  def reduceByKeyDP_Tuple(func: (V, V) => V, app_name: String, k_dist: Int): RDD[(K,V)] = {
    val array = reduceByKeyDP_deep(func)
    val ls = array._1.map(p => {
      val lls = p._2.map(q => {
        val qtuples = q.asInstanceOf[RDD[(Double, Double, Double, Double, Double, Int)]]
        val result = array._3.asInstanceOf[RDD[(K, (Double, Double, Double, Double, Double, Int))]]
        val this_key = result.lookup(p._1)(0)
        val result_broadcast = result.sparkContext.broadcast(this_key)
        val max = qtuples
          .asInstanceOf[RDD[(Double, Double, Double, Double, Double, Int)]]
          .reduce((a, b) => {
            val b_result = result_broadcast.value
            val first = scala.math.max(scala.math.abs(a._1 - b_result._1), scala.math.abs(b._1 - b_result._1))
            val second = scala.math.max(scala.math.abs(a._2 - b_result._2), scala.math.abs(b._2 - b_result._2))
            val third = scala.math.max(scala.math.abs(a._3 - b_result._3), scala.math.abs(b._3 - b_result._3))
            val forth = scala.math.max(scala.math.abs(a._4 - b_result._4), scala.math.abs(b._4 - b_result._4))
            val fifth = scala.math.max(scala.math.abs(a._5 - b_result._5), scala.math.abs(b._5 - b_result._5))
            val sixth = scala.math.max(scala.math.abs(a._6 - b_result._6), scala.math.abs(b._6 - b_result._6))
            (first, second, third, forth, fifth, sixth)
          })
        max
      })
      val tupleArray_in = lls
      var max_nls_1 = 0.0
      var max_nls_2 = 0.0
      var max_nls_3 = 0.0
      var max_nls_4 = 0.0
      var max_nls_5 = 0.0
      var max_nls_6 = 0
      for (i <- 0 until tupleArray_in.length) {
        if(tupleArray_in(i)._1*exp(-beta*(i+1)) > max_nls_1)
          max_nls_1 = tupleArray_in(i)._1
        if(tupleArray_in(i)._2*exp(-beta*(i+1)) > max_nls_2)
          max_nls_2 = tupleArray_in(i)._2
        if(tupleArray_in(i)._3*exp(-beta*(i+1)) > max_nls_3)
          max_nls_3 = tupleArray_in(i)._3
        if(tupleArray_in(i)._4*exp(-beta*(i+1)) > max_nls_4)
          max_nls_4 = tupleArray_in(i)._4
        if(tupleArray_in(i)._5*exp(-beta*(i+1)) > max_nls_5)
          max_nls_5 = tupleArray_in(i)._5
        if(tupleArray_in(i)._6*exp(-beta*(i+1)).toInt > max_nls_6)
          max_nls_6 = tupleArray_in(i)._6
      }
      (p._1,(max_nls_1,max_nls_2,max_nls_3,max_nls_4,max_nls_5,max_nls_6))
    })

    val ls_advance = array._2.map(p => {//each key
      val lls = p._2.map(q => {
        val qtuples = q.asInstanceOf[RDD[(Double, Double, Double, Double, Double, Int)]]
        val result = array._3.asInstanceOf[RDD[(K, (Double, Double, Double, Double, Double, Int))]]
        val this_key = result.lookup(p._1)(0)
        val result_broadcast = result.sparkContext.broadcast(this_key)
        val max = qtuples
          .asInstanceOf[RDD[(Double, Double, Double, Double, Double, Int)]]
          .reduce((a, b) => {
            val b_result = result_broadcast.value
            val first = scala.math.max(scala.math.abs(a._1 - b_result._1), scala.math.abs(b._1 - b_result._1))
            val second = scala.math.max(scala.math.abs(a._2 - b_result._2), scala.math.abs(b._2 - b_result._2))
            val third = scala.math.max(scala.math.abs(a._3 - b_result._3), scala.math.abs(b._3 - b_result._3))
            val forth = scala.math.max(scala.math.abs(a._4 - b_result._4), scala.math.abs(b._4 - b_result._4))
            val fifth = scala.math.max(scala.math.abs(a._5 - b_result._5), scala.math.abs(b._5 - b_result._5))
            val sixth = scala.math.max(scala.math.abs(a._6 - b_result._6), scala.math.abs(b._6 - b_result._6))
            (first, second, third, forth, fifth, sixth)
          })
        max
      })
      val tupleArray_in = lls
      var max_nls_1 = 0.0
      var max_nls_2 = 0.0
      var max_nls_3 = 0.0
      var max_nls_4 = 0.0
      var max_nls_5 = 0.0
      var max_nls_6 = 0
      for (i <- 0 until tupleArray_in.length) {
        if(tupleArray_in(i)._1*exp(-beta*(i+1)) > max_nls_1)
          max_nls_1 = tupleArray_in(i)._1
        if(tupleArray_in(i)._2*exp(-beta*(i+1)) > max_nls_2)
          max_nls_2 = tupleArray_in(i)._2
        if(tupleArray_in(i)._3*exp(-beta*(i+1)) > max_nls_3)
          max_nls_3 = tupleArray_in(i)._3
        if(tupleArray_in(i)._4*exp(-beta*(i+1)) > max_nls_4)
          max_nls_4 = tupleArray_in(i)._4
        if(tupleArray_in(i)._5*exp(-beta*(i+1)) > max_nls_5)
          max_nls_5 = tupleArray_in(i)._5
        if(tupleArray_in(i)._6*exp(-beta*(i+1)).toInt > max_nls_6)
          max_nls_6 = tupleArray_in(i)._6
      }
      (p._1,(max_nls_1,max_nls_2,max_nls_3,max_nls_4,max_nls_5,max_nls_6))
    })

    val sensitivity_array = ls ++ ls_advance
    val sensitivity = array._2.head._2.head.sparkContext.parallelize(sensitivity_array)
      .reduceByKey((a,b) => (scala.math.max(a._1,b._1),scala.math.max(a._2,b._2),scala.math.max(a._3,b._3),scala.math.max(a._4,b._4),scala.math.max(a._5,b._5),scala.math.max(a._6,b._6)))
      .collect()

    array._3
  }

  def filterDPKV(f: ((K,V)) => Boolean) : dpobjectKV[K, V] = {

    val t1 = System.nanoTime
    val r1 = inputsample.filter(f)
    val r2 = inputsample_advance.filter(f)
    val r3 = inputoriginal.filter(f)
    val duration = (System.nanoTime - t1) / 1e9d
    println("filter: " + duration)
    new dpobjectKV(r1,r2,r3)
  }
  //********************Join****************************
  def joinDP[W](otherDP: RDD[(K, W)]): dpobjectArray[((K, (W, V)))] = {

    //No need to care about sample2 join sample1
    val t1 = System.nanoTime

    val joinresult = original.join(otherDP).map(q => (q._1,(q._2._2,q._2._1)))

    val advance_original = sample_advance
      .zipWithIndex()
      .map(p => (p._1._1,(p._1._2,p._2)))
      .join(otherDP)
      .map(p => ((p._1,(p._2._2,p._2._1._1)),p._2._1._2))

    val with_sample = sample
      .zipWithIndex()
      .map(p => (p._1._1,(p._1._2,p._2)))
      .join(otherDP)
      .map(p => ((p._1,(p._2._2,p._2._1._1)),p._2._1._2))

    val duration = (System.nanoTime - t1) / 1e9d
    println("join: " + duration)

    new dpobjectArray(with_sample,advance_original,joinresult)
  }

  def joinDP_original[W](otherDP: RDD[(K, W)]): dpobjectArray[((K, (V, W)))] = {

    //No need to care about sample2 join sample1
    val t1 = System.nanoTime

    val joinresult = original.join(otherDP).map(q => (q._1,(q._2._1,q._2._2)))

    val advance_original = sample_advance
      .zipWithIndex()
      .map(p => (p._1._1,(p._1._2,p._2)))
      .join(otherDP)
      .map(p => ((p._1,(p._2._1._1,p._2._2)),p._2._1._2))

    val with_sample = sample
      .zipWithIndex()
      .map(p => (p._1._1,(p._1._2,p._2)))
      .join(otherDP)
      .map(p => ((p._1,(p._2._1._1,p._2._2)),p._2._1._2))

    val duration = (System.nanoTime - t1) / 1e9d
    println("join: " + duration)

    new dpobjectArray(with_sample,advance_original,joinresult)
  }

  def joinDP[W](otherDP: dpobjectKV[K, W]): dpobjectArray[((K, (V, W)))] = {

    //No need to care about sample2 join sample1
    val t1 = System.nanoTime

    val input2 = otherDP.original
    val input2_sample = otherDP.original
    val joinresult = original.join(otherDP.original)

    val zipin_advance = otherDP.sample_advance
      .zipWithIndex()
      .map(p => (p._1._1,(p._1._2,p._2)))
    val original_advance = original
      .join(zipin_advance)
      .map(p => ((p._1,(p._2._1,p._2._2._1)),p._2._2._2))

    val advance_original = sample_advance
      .zipWithIndex()
      .map(p => (p._1._1,(p._1._2,p._2)))
      .join(otherDP.original)
      .map(p => ((p._1,(p._2._1._1,p._2._2)),p._2._1._2))
    //        val advance_advance = sample_advance.join(otherDP.sample_advance)


    val with_sample = sample
      .zipWithIndex()
      .map(p => (p._1._1,(p._1._2,p._2)))
      .join(input2)
      .map(p => ((p._1,(p._2._1._1,p._2._2)),p._2._1._2))

    val zipin = otherDP.sample
      .zipWithIndex()
      .map(p => (p._1._1,(p._1._2,p._2)))
    val with_input2_sample = original
      .join(zipin)
      .map(p => ((p._1,(p._2._1,p._2._2._1)),p._2._2._2))

    //        val samples_join = sample.join(input2_sample)

    //        print("array1: ")
    //        with_sample.union(with_input2_sample).union(samples_join).groupByKey.collect().foreach(println)
    //        print("array2: ")
    //          original_advance.union(advance_original).union(advance_advance).groupByKey.collect().foreach(println)

    //This is final original result because there is no inter key
    //or intra key combination for join i.e., no over lapping scenario
    //within or between keys
    val duration = (System.nanoTime - t1) / 1e9d
    println("join: " + duration)
    new dpobjectArray(with_sample ++ with_input2_sample,original_advance ++ advance_original,joinresult)

  }
}