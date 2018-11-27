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
  extends RDD[T] (rdd1)
{
  var sample = rdd1
  var main = rdd1

  override def compute(split: org.apache.spark.Partition,context: org.apache.spark.TaskContext): Iterator[T] =
  {
    rdd1.iterator(split, context)
  }

  override protected def getPartitions: Array[org.apache.spark.Partition] =
    rdd1.partitions

  def mapDP[U: ClassTag](f: T => U): dpobject[U]= {
//    main match {
//      case a: RDD[Int] =>
    //Normal Sample is ok e.g., tuple
        val mainresult = main
        val sampling = mainresult.sample(false,0.2)
        new dpobject(sampling.map(f),mainresult.subtract(sampling).map(f))
//    }
  }

  def mapDPKV[K: ClassTag,V: ClassTag](f: T => (K,V)): dpobjectKV[K,V]= {
    val mainresult = main
    val sampling = mainresult.sample(false,0.2)
    new dpobjectKV(sampling.map(f).asInstanceOf[RDD[(K,V)]], mainresult.subtract(sampling).map(f).asInstanceOf[RDD[(K,V)]])
  }
}
