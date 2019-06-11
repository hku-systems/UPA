package edu.hku.cs.dp

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.{MapPartitionsRDD, RDD, RDDOperationScope}

import scala.reflect.ClassTag


/**
  * Created by lionon on 10/22/18.
  */
class dpread[T: ClassTag](
  var rdd1 : RDD[T])
{
  var main = rdd1
  var advance = rdd1

  def mapDP[U: ClassTag](f: T => U): dpobject[U]= {
//    main match {
//      case a: RDD[Int] =>
    //Normal Sample is ok e.g., tuple
//        val sample_rate = 1111/main.count()
val t1 = System.nanoTime
    val sampling = main.sparkContext.parallelize(main.takeSample(false, 1111))
        val advance_sampling = advance.sparkContext.parallelize(advance.takeSample(false, 1111))
        val duration = (System.nanoTime - t1) / 1e9d
        println("sample: " + duration)
        new dpobject(sampling.map(f),advance_sampling.map(f),main.map(f))
//    }
  }

  def mapDP[U: ClassTag](f: T => U, rate: Int): dpobject[U]= {
    //    main match {
    //      case a: RDD[Int] =>
    //Normal Sample is ok e.g., tuple
    //        val sample_rate = 1111/main.count()
//    val t1 = System.nanoTime
    val t1 = System.nanoTime
    val sampling = main.sparkContext.parallelize(main.take(rate))
    val advance_sampling = advance.sparkContext.parallelize(advance.take(rate))
    val duration = (System.nanoTime - t1) / 1e9d
    println("sample: " + duration)
//    val duration = (System.nanoTime - t1) / 1e9d
//    print("Sample: " + duration)
    new dpobject(sampling.map(f),advance_sampling.map(f),main.map(f))
    //    }
  }

  def mapDPKV[K: ClassTag,V: ClassTag](f: T => (K,V)): dpobjectKV[K,V]= {
//    val t1 = System.nanoTime
    val t1 = System.nanoTime
    val sampling = main.sparkContext.parallelize(main.takeSample(false, 1111))
    val advance_sampling = advance.sparkContext.parallelize(advance.takeSample(false, 1111))
    val duration = (System.nanoTime - t1) / 1e9d
    println("sample: " + duration)
//    val duration = (System.nanoTime - t1) / 1e9d
//    print("Sample: " + duration)
    new dpobjectKV(sampling.map(f).asInstanceOf[RDD[(K,V)]],advance_sampling.map(f).asInstanceOf[RDD[(K,V)]],main.map(f).asInstanceOf[RDD[(K,V)]])
  }
}
