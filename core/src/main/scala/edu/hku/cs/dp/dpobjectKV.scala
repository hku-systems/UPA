package edu.hku.cs.dp

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD

import scala.collection.immutable.HashMap
import scala.collection.immutable.HashSet
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

//      [automated, efficient and precise] [sensitivity] in DISC


//      def filterDPKV(f: (K,V) => Boolean) : dpobjectKV[K,V] = {
//        new dpobjectKV(inputsample.filter(f), inputoriginal.filter(f))
//      }

      def reduceByKeyDP(func: (V, V) => V): dpobject[(K,V)] = {
//reduce shuffle, use map rather than reduce/reduceByKey
        val originalresult = inputoriginal.reduceByKey(func)//Reduce original first
        val originalfinalresult = sample.union(originalresult).reduceByKey(func)
        val broadcastOriginal = sample.sparkContext.broadcast(HashMap() ++ originalresult.groupByKey.collect())//Unique Key
        val broadcastSample = sample.sparkContext.broadcast(HashMap() ++ sample.groupByKey.collect())//May not be UniqueKey
        val withoutSample = sample.map(p => {//"sample" means the aggregated result without that record
          val option2list1 = broadcastSample.value.get(p._1).toList
          val option2list2 = broadcastOriginal.value.get(p._1).toList
          val option2list = option2list1 ++ option2list2
        val s = HashSet() ++ option2list.flatMap(l => l) //join value in sample and result of a key, should take short time if not many element in those keys
        val ss = s - p._2
          (p._1,(s.reduce(func)))
        })
        new dpobject(withoutSample,originalfinalresult)
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
