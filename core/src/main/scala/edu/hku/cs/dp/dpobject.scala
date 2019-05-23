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
  var inputoriginal : RDD[T])
  extends RDD[T](inputoriginal)
{

  var sample = inputsample //for each element, sample refers to "if this element exists"
  var original = inputoriginal
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
    new dpobject(inputsample.map(f), inputoriginal.map(f))
  }

  def mapDPKV[K: ClassTag,V: ClassTag](f: T => (K,V)): dpobjectKV[K,V]= {
    new dpobjectKV(inputsample.map(f).asInstanceOf[RDD[(K,V)]], inputoriginal.map(f).asInstanceOf[RDD[(K,V)]])
  }

  def reduceDP(f: (T, T) => T) : (Array[RDD[T]],T) = {
    //The "sample" field carries the aggregated result already

    val result = original.reduce(f)
    val aggregatedResult = f(sample.reduce(f),result)//get the aggregated result

//    val inner = new ArrayBuffer[V]
    var inner_num = 0
    var outer_num = k_distance
    val sample_count = sample.count //e.g., 64
    val broadcast_sample = original.sparkContext.broadcast(sample.collect())
    if (sample_count <= 1) {
      val inner = new Array[RDD[T]](1) //directly return
      if(sample_count == 0)
        inner(0) = original.sparkContext.parallelize(Seq(result))
      else
        inner(0) = sample
      (inner,aggregatedResult)
    }
    else {
      if(sample_count <= k_distance - 1)
        outer_num = k_distance - 1
      else
        outer_num = k_distance //outer_num = 8
      var array = new Array[RDD[T]](outer_num)
      var i = outer_num
      while(i  > 0) {
          val up_to_index = (sample_count - i).toInt
        if(i == outer_num) {
          println("sample_count: " + sample_count)
          println("outer-most loop: " + up_to_index)

          val inner_array = original.sparkContext.parallelize(0 to up_to_index - 1) //(0,1,2,3,4,5,6,7)
            .map(p => {
            val inside_array = broadcast_sample.value.patch(p, Nil, i)
            f(inside_array.reduce(f),result) //(0 -> 7, 8 -> 15, 16, 24, 32, 40, 48, 56)
          })
          array(i - 1) = inner_array
        } else {
          val array_collected = array(i).collect()
          val upper_array = original.sparkContext.broadcast(array_collected)

          val array_length = array_collected.length
          val array_length_broadcast = original.sparkContext.broadcast(array_length)
          val up_to_index = (sample_count - i).toInt

          println("sample_count: " + sample_count)
          println("array_length: " + array_length)
          println("current i: " + i)
          println("outer_num: " + outer_num)
          println("up_to_index: " + up_to_index)

          val inner_array = original.sparkContext.parallelize(0 to up_to_index - 1) //(0,1,2,3,4,5,6,7)
            .map(p => {
            if(p < array_length_broadcast.value) {
              val ss = upper_array.value(p)
              val sss = broadcast_sample.value(p + i + 1)
              f(f(upper_array.value(p), broadcast_sample.value(p + i + 1)), result)
            }
            else if(p == array_length_broadcast)
              f(f(upper_array.value(p - 1),broadcast_sample.value(p)),result)
            else {
              val inside_array = broadcast_sample.value.patch(p, Nil, i)
              f(inside_array.reduce(f),result)
            }
          })
          array(i - 1) = inner_array
        }
        i = i - 1
      }
      (array,aggregatedResult)
    }
  }

def reduce_and_add_noise_KDE(f: (T, T) => T): (T,T) = {
  //computin candidates of smooth sensitivity
val array = reduceDP(f).asInstanceOf[(Array[RDD[Double]],Double)]
  val neigbour_local_senstivity = array._1.map(p => {
    val max = p.max
    val min = p.min
    max - min
  })
    var max_nls = 0.0
  for (i <- 0 until neigbour_local_senstivity.length) {
    neigbour_local_senstivity(i) = neigbour_local_senstivity(i)*exp(-beta*(i+1))
    if(neigbour_local_senstivity(i) > max_nls)
      max_nls = neigbour_local_senstivity(i)
  }
  (max_nls.asInstanceOf[T],array._2.asInstanceOf[T]) //sensitivity
}

  def reduce_and_add_noise_LR(f: (T, T) => T): (T,T) = {
    //computin candidates of smooth sensitivity
    val array = reduceDP(f).asInstanceOf[(Array[RDD[Vector[Double]]],Vector[Double])]
    val vector_length = array._2.length
    val neigbour_local_senstivity = array._1.map(p => {
      val max = p.reduce((a,b) => {
        a.zip(b).map(q => scala.math.max(q._1,q._2))
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
    (max_nls.asInstanceOf[T],array._2.asInstanceOf[T]) //sensitivity
  }

def filterDP(f: T => Boolean) : dpobject[T] = {
  new dpobject(inputsample.filter(f), inputoriginal.filter(f))
}

  def addnoiseQ31(): Unit = {
    val q31DP = sample.asInstanceOf[RDD[((String,String),(Double,Double,Double,Double,Double,Int))]]
    val get_max = q31DP.reduceByKey((a,b) => (scala.math.max(a._1,b._1),scala.math.max(a._2,b._2),scala.math.max(a._3,b._3),scala.math.max(a._4,b._4),scala.math.max(a._5,b._5),scala.math.max(a._6,b._6)))
    println("***********computing min***********")
    val get_min = q31DP.reduceByKey((a,b) => (scala.math.min(a._1,b._1),scala.math.min(a._2,b._2),scala.math.min(a._3,b._3),scala.math.min(a._4,b._4),scala.math.min(a._5,b._5),scala.math.min(a._6,b._6)))
    println("***********computing lambda***********")
    val get_lambda = get_max.union(get_min).reduceByKey((a,b) => (scala.math.abs(a._1 - b._1), scala.math.abs(a._2 - b._2), scala.math.abs(a._3 - b._3), scala.math.abs(a._4 - b._4),scala.math.abs(a._5 - b._5),scala.math.abs(a._6 - b._6)))
    print("Sensitvity is: " + get_lambda.collect().foreach(p => print(p._1._1 + "," + p._1._2 + ":" + p._2._1 + "," + p._2._2 + "," + p._2._3 + "," + p._2._4 + "," + p._2._5 + "," + p._2._6 + "\n")))
    print("Original result is: " + original.collect.foreach(println))
//      .asInstanceOf[RDD[((String,String),(Double,Double,Double,Double,Double,Int))]]
//      .collect()
//      .foreach(p => print(p._1._1 + "," + p._1._2 + ":" + p._2._1 + "," + p._2._2 + "," + p._2._3 + "," + p._2._4 + "," + p._2._5 + "," + p._2._6 + "\n")))
  }

  def addnoiseQ34(): Unit = {
    val q34DP = sample.asInstanceOf[RDD[((String,String),Int)]]
    val get_max = q34DP.reduceByKey((a,b) => (scala.math.max(a,b)))
    println("***********computing min***********")
    val get_min = q34DP.reduceByKey((a,b) => (scala.math.min(a,b)))
    println("***********computing lambda***********")
    val get_lambda = get_max.union(get_min).reduceByKey((a,b) => (scala.math.abs(a - b)))
    print("Sensitvity is: " + get_lambda.collect().foreach(p => print(p._1._1 + "," + p._1._2 + ":" + p._2)))
    print("Original result is: " + original.asInstanceOf[RDD[((String,String),Int)]].collect().foreach(p => print(p._1._1 + "," + p._1._2 + ":" + p._2)))
  }

  def addnoiseQ41(): Unit = {
    val q34DP = sample.asInstanceOf[RDD[((Long,Long),Int)]]
    val get_max = q34DP.reduceByKey((a,b) => (scala.math.max(a,b)))
    println("***********computing min***********")
    val get_min = q34DP.reduceByKey((a,b) => (scala.math.min(a,b)))
    println("***********computing lambda***********")
    val get_lambda = get_max.union(get_min).reduceByKey((a,b) => (scala.math.abs(a - b)))
    print("Sensitvity is: " + get_lambda.collect().foreach(p => print(p._1._1 + "," + p._1._2 + ":" + p._2)))
    print("Original result is: " + original.asInstanceOf[RDD[((Long,Long),Int)]].collect().foreach(p => print(p._1._1 + "," + p._1._2 + ":" + p._2)))
  }

  def addnoiseQ46(): Unit = {
    val q34DP = sample.asInstanceOf[RDD[((String,String,Long),Int)]]
    val get_max = q34DP.reduceByKey((a,b) => (scala.math.max(a,b)))
    println("***********computing min***********")
    val get_min = q34DP.reduceByKey((a,b) => (scala.math.min(a,b)))
    println("***********computing lambda***********")
    val get_lambda = get_max.union(get_min).reduceByKey((a,b) => (scala.math.abs(a - b)))
    print("Sensitvity is: " + get_lambda.collect().foreach(p => print(p._1._1 + "," + p._1._2 + p._1._3 +  ":" + p._2)))
    print("Original result is: " + original.asInstanceOf[RDD[((String,String,Long),Int)]].collect().foreach(p => print(p._1._1 + "," + p._1._2 + p._1._3 +  ":" + p._2)))
  }

  def addnoiseQ51(): Unit = {
    val q34DP = sample.asInstanceOf[RDD[(String,Int)]]
    val get_max = q34DP.reduceByKey((a,b) => (scala.math.max(a,b)))
    println("***********computing min***********")
    val get_min = q34DP.reduceByKey((a,b) => (scala.math.min(a,b)))
    println("***********computing lambda***********")
    val get_lambda = get_max.union(get_min).reduceByKey((a,b) => (scala.math.abs(a - b)))
    print("Sensitvity is: " + get_lambda.collect().foreach(p => print(p._1 + ":" + p._2)))
    print("Original result is: " + original.asInstanceOf[RDD[(String,Int)]].collect().foreach(p => print(p._1 + ":" + p._2)))
  }

  def addnoise(): Any = {
    println("***********Adding noise***********")
    sample.asInstanceOf[Any] match {
      case intsample : RDD[Int] =>
        val sorted = intsample.sortBy(p => p)
        val max = sorted.max
        val min = sorted.min
        val laprand = max - min
        println("Int The input sample is:")
        sorted.map(p => println(p))
        println("Int max: " + max)
        println("Int min: " + min)
        println("Sensitivity: " + laprand)
//        original.asInstanceOf[RDD[Int]].collect().head.toDouble + laprand.toDouble
      case doublesample : RDD[Double] =>
        val sorted = doublesample.sortBy(p => p)
        val max = sorted.max
        val min = sorted.min
        println("Double The input sample is:")
        sorted.map(p => println(p))
        println("Double max: " + max)
        println("Double min: " + min)
//        original.asInstanceOf[RDD[Double]].collect().head + max - min
      case threeIntTuplesample : RDD[(Int,(Int,Int))] =>
        val sorted1 = threeIntTuplesample.map(p => p._2._1)
        val max1 = sorted1.max
        val min1 = sorted1.min
        val laprand1 = max1 - min1
        val sorted2 = threeIntTuplesample.map(p => p._2._1)
        val max2 = sorted2.max
        val min2 = sorted2.min
        val laprand2 = max2 - min2
      case q31DP : RDD[((String,String),(Double,Double,Double,Double,Double,Int))] =>
        println("***********computing max***********")
        val get_max = q31DP.reduceByKey((a,b) => (scala.math.max(a._1,b._1),scala.math.max(a._2,b._2),scala.math.max(a._3,b._3),scala.math.max(a._4,b._4),scala.math.max(a._5,b._5),scala.math.max(a._6,b._6)))
        println("***********computing min***********")
        val get_min = q31DP.reduceByKey((a,b) => (scala.math.min(a._1,b._1),scala.math.min(a._2,b._2),scala.math.min(a._3,b._3),scala.math.min(a._4,b._4),scala.math.min(a._5,b._5),scala.math.min(a._6,b._6)))
        println("***********computing lambda***********")
        val get_lambda = get_max.union(get_min).reduceByKey((a,b) => (scala.math.abs(a._1 - b._1), scala.math.abs(a._2 - b._2), scala.math.abs(a._3 - b._3), scala.math.abs(a._4 - b._4),scala.math.abs(a._5 - b._5),scala.math.abs(a._6 - b._6)))
        print("Sensitvity is: " + get_lambda.collect())
        print("Original result is: " + original.collect())
      case _ =>
        println("no match")

    }
//    result
  }

}
