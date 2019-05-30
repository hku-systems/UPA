package edu.hku.cs.dp

import org.apache.spark.rdd.RDD

import scala.math.{exp, pow}
import scala.reflect.ClassTag

/**
  * Created by lionon on 10/22/18.
  */
class dpobjectArray[T: ClassTag](
  var inputsample : Array[RDD[T]],
  var inputsample_advance : Array[RDD[T]],
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
    val r1 = inputsample.map(p => p.map(f))
    val r2 = sample_advance.map(p => p.map(f))
    val r3 = inputoriginal.map(f)
    val duration = (System.nanoTime - t1) / 1e9d
    print("mapDP: " + duration)
    new dpobjectArray(r1,r2,r3)
  }

  def mapDPKV[K: ClassTag,V: ClassTag](f: T => (K,V)): dpobjectKVArray[K,V]= {
    val t1 = System.nanoTime
    val r1 = inputsample.map(p => p.map(f)).asInstanceOf[Array[RDD[(K,V)]]]
    val r2 = sample_advance.map(p => p.map(f)).asInstanceOf[Array[RDD[(K,V)]]]
    val r3 = inputoriginal.map(f).asInstanceOf[RDD[(K,V)]]
    val duration = (System.nanoTime - t1) / 1e9d
    print("mapDP: " + duration)
    new dpobjectKVArray(r1,r2,r3)
  }

  def filterDP(f: T => Boolean) : dpobjectArray[T] = {
    val t1 = System.nanoTime
    val r1 = inputsample.map(p => p.filter(f))
    val r2 = inputsample_advance.map(p => p.filter(f))
    val r3 = inputoriginal.filter(f)
    val duration = (System.nanoTime - t1) / 1e9d
    print("filterDP: " + duration)
    new dpobjectArray(r1,r2,r3)
  }

  def reduceDP(f: (T, T) => T) : (Array[RDD[T]],Array[RDD[T]],T) = {
    //The "sample" field carries the aggregated result already
    val t1 = System.nanoTime
    val result = original.reduce(f)
    val filtered_sample = sample.filter(p => !p.isEmpty)
    val filtered_sample_advance = sample_advance.filter(p => !p.isEmpty)
    val aggregatedResult = f(filtered_sample.map(p => p.reduce(f)).reduce(f),result)//get the aggregated result
    val broadcast_result = original.sparkContext.broadcast(result)
    val broadcast_aggregatedResult = original.sparkContext.broadcast(aggregatedResult)

    //    val inner = new ArrayBuffer[V]
    var inner_num = 0
    var outer_num = k_distance
    val sample_count = filtered_sample.length //e.g., 64
    val sample_advance_count = filtered_sample_advance.length
    val broadcast_sample = original.sparkContext.broadcast(filtered_sample.map(p => p.collect()))
    val broadcast_sample_advance = original.sparkContext.broadcast(filtered_sample_advance.map(p => p.collect()))
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
          val b_i = original.sparkContext.broadcast(i)
          val inner_array = original.sparkContext.parallelize(0 to up_to_index - 1) //(0,1,2,3,4,5,6,7)
            .map(p => {
            f(broadcast_sample.value.patch(p, Nil, b_i.value).flatten.reduce(f),broadcast_result.value) //(0 -> 7, 8 -> 15, 16, 24, 32, 40, 48, 56)
          })
//          println("i is " + i)
          array(i - 1) = inner_array
        } else {
          val array_collected = array(i).collect()
          val upper_array = original.sparkContext.broadcast(array_collected)
          val b_i = original.sparkContext.broadcast(i)
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
              f(upper_array.value(p), broadcast_sample.value(p + b_i.value + 1).reduce(f))//no need to include result, as it is included //no need to plus one?
            }
//            else if(p == array_length_broadcast.value)
//              f(upper_array.value(p - 1),broadcast_sample.value(p))//redundant, use upper_array.value(p) twice, means doing the same thing
            else {
              val inside_array = broadcast_sample.value.patch(p, Nil, b_i.value).flatten
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
        array_advance(0) = original.sparkContext.parallelize(Seq(f(aggregatedResult,sample_advance.head.reduce(f))))
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
            f(inside_array.reduce(f),broadcast_aggregatedResult.value) //(0 -> 7, 8 -> 15, 16, 24, 32, 40, 48, 56)
          })
          array_advance(i) = inner_array
        } else {
          val up_to_index = (sample_advance_count - i).toInt
          val array_collected = array_advance(i - 1).collect()
          val upper_array = original.sparkContext.broadcast(array_collected)
          val b_i = original.sparkContext.broadcast(i)

          val inner_array = original.sparkContext.parallelize(0 to up_to_index - 1) //(0,1,2,3,4,5,6,7)
            .map(p => {
              f(upper_array.value(p), broadcast_sample_advance.value(p + b_i.value).reduce(f))//no need to include result, as it is included
          })
          array_advance(i) = inner_array
        }
        i = i + 1
      }
    }
    val duration = (System.nanoTime - t1) / 1e9d
    print("reduceDP: " + duration)
    (array,array_advance,aggregatedResult)
  }

def reduce_and_add_noise_KDE(f: (T, T) => T, app_name: String, k_dist: Int): T = {
  //computin candidates of smooth sensitivity
var array = reduceDP(f).asInstanceOf[(Array[RDD[Double]],Array[RDD[Double]],Double)]
  val t1 = System.nanoTime

  var a1_length = array._1.length
  var neigbour_local_senstivity = new Array[Double](a1_length)
  for(a1 <- 0 until array._1.length)
    {
      val p = array._1(a1)
      if(!p.isEmpty) {
        val max = p.max
        val min = p.min
        val mean = p.mean
        val sd = p.stdev
        val counting = p.count()
        println(app_name + "," + k_dist + "," + ((a1 + 1) * (-1)) + "," + mean + "," + sd + "," + counting)
        neigbour_local_senstivity(a1) = scala.math.max(scala.math.abs(max - array._3), scala.math.abs(min - array._3))
      }
      else {
        println(app_name + "," + k_dist + "," + ((a1 + 1) * (-1)) + "," + array._3 + "," + 0 + "," + 0)
        neigbour_local_senstivity(a1) = 0.0
      }
    }

  var a2_length = array._2.length
  var neigbour_local_advance_senstivity = new Array[Double](a2_length)
  for(a2 <- 0 until a2_length)
  {
    val p = array._2(a2)
    if(!p.isEmpty) {
      val max = p.max
      val min = p.min
      val mean = p.mean
      val sd = p.stdev
      val counting = p.count()
      println(app_name + "," + k_dist + "," + (a2 + 1) + "," + mean + "," + sd + "," + counting)
      neigbour_local_advance_senstivity(a2) = scala.math.max(scala.math.abs(max - array._3), scala.math.abs(min - array._3))
    }
    else {
      println(app_name + "," + k_dist + "," + (a2 + 1) + "," + array._3 + "," + 0 + "," + 0)
      neigbour_local_advance_senstivity(a2) = 0.0
    }
  }


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
  val duration = (System.nanoTime - t1) / 1e9d
  print("smooth sensitivity: " + duration)
  array._3.asInstanceOf[T] //sensitivity
}

}
