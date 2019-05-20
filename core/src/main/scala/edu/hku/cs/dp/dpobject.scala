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

  def reduceDP(f: (T, T) => T) : T = {
    //The "sample" field carries the aggregated result already

    val result = original.reduce(f)
    val aggregatedResult = f(sample.reduce(f),result)//get the aggregated result
    val samplecollected = sample.collect()//collect sample to local
    val tmp = HashSet() ++ samplecollected
//    val broadcastvar = sample.sparkContext.broadcast(tmp)

//    val inner = new ArrayBuffer[V]
    var inner_num = 0
    var outer_num = 8
    val sample_count = sample.count //e.g., 64
    val broadcast_sample = original.sparkContext.broadcast(sample.collect())
    if (sample_count <= 1) {
      sample //directly return
    }
    else {
      if(sample_count <= 7)
        outer_num = sample_count / sample_count - 1
      else
        outer_num = sample_count / 8 //outer_num = 8
      var array = new Array[Array[T]](outer_num)
      var i = outer_num
      while(i  > 0) {
          val up_to_index = sample_count / i// 8
        if(i == outer_num) {
          val inner_array = original.sparkContext.parallelize(0 to up_to_index - 1) //(0,1,2,3,4,5,6,7)
            .map(p => {
            val inside_array = broadcast_sample.value.slice(p, p + i)
            inside_array.reduce(f) //(0 -> 7, 8 -> 15, 16, 24, 32, 40, 48, 56)
          }).collect()
          array[i - 1] = inner_array
        } else {
          val inner_array = original.sparkContext.parallelize(0 to up_to_index - 1) //(0,1,2,3,4,5,6,7)
            .map(p => {
            f(array(i)(p),broadcast_sample.value(p + i - 1))
          }).collect()
          array[i - 1] = inner_array
        }
        i = i - 1
      }
    }


//    val withoutSample = sample.map(p => {//"sample" means the aggregated result without that record
//      val s = broadcastvar.value - p
//      f(s.reduce(f),result)
//    })

    (withoutSample, aggregatedResult)
  }

//  def takeSampleDP(
//                  withReplacement: Boolean,
//                  num: Int,
//                  seed: Long = Utils.random.nextLong): Array[T] = {//sample the original is sufficient because for the sample one we want them to be there to form neighbouring datasets
//      inputoriginal.takeSample(withReplacement,num,seed)
//    }
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
