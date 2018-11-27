package edu.hku.cs.dp

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

/**
  * Created by lionon on 10/28/18.
  */
class dpobjectKV[K, V](var inputsample: RDD[(K, V)], var inputoriginal: RDD[(K, V)])
                       (implicit kt: ClassTag[K], vt: ClassTag[V], ord: Ordering[K] = null)
  extends Logging with Serializable
    {
      var sample = inputsample
      var original = inputoriginal

      //*******************ReduceByKey****************************

//      def collectAsMapDP(): dplocalMap[K,V] = {
//
//        val collectedMap = inputoriginal.collectAsMap()
//
//        val sampleWithMap = inputsample.map(p => {
//          val map = collection.mutable.Map(p._1 -> p._2) ++ collectedMap
//          map
//        })
//
//        new dplocalMap[K,V](sampleWithMap)
//      }


      def reduceByKeyDP(func: (V, V) => V): dpobject[(K,V)] = {

        val originalresult = inputoriginal.reduceByKey(func)//Reduce original first

        val allkeys = sample.keys.collect()//collect keys from sample

//        println("reduceByKeyDP: allkeys")
//        allkeys.foreach(println)
//        println("End of allkeys")

        var allKVofRDD = allkeys.map( q => {
          val rddvalue = original.lookup(q).head // can use head because after reduce, each key should only associate with 1 element
          (q,rddvalue)
        }).toMap

        val b1 = sample.sparkContext.broadcast(allKVofRDD)

        val result = sample.reduceByKey(func).map(p => {
          val value = b1.value.getOrElse(p._1, p._2).asInstanceOf[V]
          (p._1,func(value,p._2))
        })

        new dpobject(result,originalresult)
      }

      //********************Join****************************

      def joinDP[W](otherDP: dpobjectKV[K, W]): dpobjectKV[K, (V, W)] = {

        //No need to care about sample2 join sample1

        val joinresult = original.join(otherDP.original)
        val self = sample
        val other = otherDP.sample
        val input2 = otherDP.original

        val allselfkeys = self.keys.collect().toSeq //the number of keys may be much less than the number of sampled elements
        val allvaluesforjoin = allselfkeys.map(key => {//Can I run this distributively? the main challenge is RDD cannot access RDD

          val resultvalues = joinresult.partitioner match {
            case Some(p) =>
              val index = p.getPartition(key)
              val process = (it: Iterator[(K, (V,W))]) => {
                val buf = new ArrayBuffer[(V,W)]
                for (pair <- it if pair._1 == key) {
                  buf += pair._2
                }
                buf
              } : Seq[(V,W)]
              val res = self.context.runJob(joinresult, process, Array(index))
              res(0).asInstanceOf[Seq[(V,W)]]
            case None =>
              self.filter(_._1 == key).map(_._2).collect().toSeq.asInstanceOf[Seq[(V,W)]]
          } //Use lookup -> save memory (as look up from disk)

          val samplevalues = other.partitioner match {
            case Some(p) =>
              val index = p.getPartition(key)
              val process = (it: Iterator[(K, W)]) => {
                val buf = new ArrayBuffer[W]
                for (pair <- it if pair._1 == key) {
                  buf += pair._2
                }
                buf
              } : Seq[W]
              val res = self.context.runJob(other, process, Array(index))
              res(0).asInstanceOf[Seq[W]]
            case None =>
              self.filter(_._1 == key).map(_._2).collect().toSeq.asInstanceOf[Seq[W]]
          } //Use lookup -> save memory (as look up from disk)

          if(!resultvalues.isEmpty)
          {
            (key,resultvalues.map(_._2) ++ samplevalues)
          }
          else
          {
            val othervalues = input2.partitioner match {
              case Some(p) =>
                val index = p.getPartition(key)
                val process = (it: Iterator[(K, W)]) => {
                  val buf = new ArrayBuffer[W]
                  for (pair <- it if pair._1 == key) {
                    buf += pair._2
                  }
                  buf
                } : Seq[W]
                val res = self.context.runJob(input2, process, Array(index))
                res(0).asInstanceOf[Seq[W]]
              case None =>
                self.filter(_._1 == key).map(_._2).collect().toSeq.asInstanceOf[Seq[W]]
            } //if the original does not consist such a key, then the whole key-value pair maybe missing, therefore, need to rejoin
            (key, samplevalues ++ othervalues)
          }
        }).toMap

        val joinvalueswithkey = self.map(p => {
          var values = allvaluesforjoin.getOrElse(p._1,p._2).asInstanceOf[Seq[W]]
          if(!values.isEmpty) {
            Some(values.map(q => {
              (p._1, (p._2,q))
            }))
          }
          else {
            None
          }
        })

        val ss = joinvalueswithkey.flatMap(p => p).flatMap(p => p)
        new dpobjectKV(ss,joinresult)
      }


}
