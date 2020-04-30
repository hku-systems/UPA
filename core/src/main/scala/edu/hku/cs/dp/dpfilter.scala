package edu.hku.cs.dp

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.{MapPartitionsRDD, RDD, RDDOperationScope}

import scala.reflect.ClassTag


/**
 * Created by lionon on 10/22/18.
 */
class dpfilter[T: ClassTag](
                           var rdd1 : RDD[T])
{
  var main = rdd1
  def mapfilter[U: ClassTag](f: T => U, rate: Int): dpfilter[U]= {
    new dpfilter(main.map(f))
  }

  def filterDP(f: T => Boolean,rate: Int) : dpobject[T] = {
    val t1 = System.nanoTime

    val r3 = main.filter(f)
    val compl = main.filter(p => !f(p))
    val r1 = main.sparkContext.parallelize(r3.take(rate)).filter(f)

    val duration = (System.nanoTime - t1) / 1e9d
    println("sample: " + duration)
    if(compl.isEmpty) {
      new dpobject(r1,compl,r3)
    }
    else {
      val r2 = main.sparkContext.parallelize(compl.take(rate))
      new dpobject(r1,r2,r3)
    }
  }
}
