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

  def mapDP[U: ClassTag](f: T => U, rate: Int): dpobject[U]= {
    //    main match {
    //      case a: RDD[Int] =>
    //Normal Sample is ok e.g., tuple
    //        val sample_rate = 1111/main.count()
//    val t1 = System.nanoTime
    val t1 = System.nanoTime
    val advance_sampling = main.sparkContext.parallelize(main.take(rate))
    val subtract_advance_main = main.subtract(advance_sampling)
    val sampling = main.sparkContext.parallelize(subtract_advance_main.take(rate))
    val duration = (System.nanoTime - t1) / 1e9d
    println("sample: " + duration)
//    val duration = (System.nanoTime - t1) / 1e9d
//    print("Sample: " + duration)
    new dpobject(sampling.map(f),advance_sampling.map(f),main.subtract(sampling).map(f))
    //    }
  }
}
