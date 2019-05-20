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

//      def mapDPKV(f: T => (K,V)): dpobjectKV[K,V]= {
//        new dpobjectKV(inputsample.map(f).asInstanceOf[RDD[(K,V)]], inputoriginal.map(f).asInstanceOf[RDD[(K,V)]])
//      }

      //parallelise inter key operation or intra key operation
      //seems use less collect is better
      //because collect then parallelise requires more rtt and sorting
      def reduceByKeyDP(func: (V, V) => V): dpobject[(K,V)] = {
//reduce shuffle, use map rather than reduce/reduceByKey
        val originalresult = inputoriginal.reduceByKey(func)//Reduce original first
        val sample_gp_key = sample.groupByKey()
//        val originalfinalresult = sample.union(originalresult).reduceByKey(func)

        //should leverage the isolation between keys
        //each aggregation process only involve on key
//        val broadcastOriginal = sample.sparkContext.broadcast(HashMap() ++ originalresult.collect())//Unique Key
//        val broadcastSample = sample.sparkContext.broadcast(HashMap() ++ sample_gp_key.collect())//May not be UniqueKey

        sample_gp_key.map(p => {
          val iter = p._2
          val key = p._1
          val inner = new ArrayBuffer[V]
          val outer = new ArrayBuffer[ArrayBuffer[V]]
          var run_length = 8
          if (p._2.size <= 8)
            run_length = p._2.size - 1

          for (i <- 1 to run_length){//to is inclusive
            val divider = p._2.size / i
            for( a <- 0 to divider) {
              val each_value = iter.toSeq.combinations(1)
            }
          }




        })

//        val without_a_sample =
//        val withoutSample = sample.map(p => {//"sample" means the aggregated result without that record
//          val option2list1 = broadcastSample.value.get(p._1).toList
//          val option2list2 = broadcastOriginal.value.get(p._1).toList
//          val option2list = option2list1 ++ option2list2 //flat because option2's value is a iterable (after using group by key)
//        val s = HashSet() ++ option2list.flatMap(l => l) //join value in sample and result of a key, should take short time if not many element in those keys
//        val ss = s - p._2
//          (p._1,(s.reduce(func)))
//        })

//val without_that_Sample = sample.flatMap { case(key, value) =>
//  broadcastOriginal.value.get(key).map { otherValue =>
//            (key, (value, otherValue))
//          }

//        }
        val final_result = originalresult.union(sample).reduceByKey(func)
        //each element of the sample has only one key
        new dpobject(originalresult,originalresult)
      }

      def filterDPKV(f: ((K,V)) => Boolean) : dpobjectKV[K, V] = {
        new dpobjectKV(inputsample.filter(f), inputoriginal.filter(f))
      }
      //********************Join****************************

      def joinDP[W](otherDP: dpobjectKV[K, W]): dpobject[(K, (V, W))] = {

        //No need to care about sample2 join sample1

        val input2 = otherDP.original
        val input2_sample = otherDP.original
        val joinresult = original.join(otherDP.original)


        val with_sample = sample.join(input2)
        val with_input2_sample = original.join(otherDP.sample)
        val samples_join = sample.join(input2_sample)

        //This is final original result because there is no inter key
        //or intra key combination for join i.e., no over lapping scenario
        //within or between keys
        new dpobject(joinresult,with_sample.union(with_input2_sample).union(samples_join))

//        val allselfkeys = self.keys.collect().toSeq //the number of keys may be much less than the number of sampled elements
//        val allvaluesforjoin = allselfkeys.map(key => {//Can I run this distributively? the main challenge is RDD cannot access RDD
//
//          val resultvalues = joinresult.partitioner match {
//            case Some(p) =>
//              val index = p.getPartition(key)
//              val process = (it: Iterator[(K, (V,W))]) => {
//                val buf = new ArrayBuffer[(V,W)]
//                for (pair <- it if pair._1 == key) {
//                  buf += pair._2
//                }
//                buf
//              } : Seq[(V,W)]
//              val res = self.context.runJob(joinresult, process, Array(index))
//              res(0).asInstanceOf[Seq[(V,W)]]
//            case None =>
//              self.filter(_._1 == key).map(_._2).collect().toSeq.asInstanceOf[Seq[(V,W)]]
//          } //Use lookup -> save memory (as look up from disk)
//
//          val samplevalues = other.partitioner match {
//            case Some(p) =>
//              val index = p.getPartition(key)
//              val process = (it: Iterator[(K, W)]) => {
//                val buf = new ArrayBuffer[W]
//                for (pair <- it if pair._1 == key) {
//                  buf += pair._2
//                }
//                buf
//              } : Seq[W]
//              val res = self.context.runJob(other, process, Array(index))
//              res(0).asInstanceOf[Seq[W]]
//            case None =>
//              self.filter(_._1 == key).map(_._2).collect().toSeq.asInstanceOf[Seq[W]]
//          } //Use lookup -> save memory (as look up from disk)
//
//          if(!resultvalues.isEmpty)
//          {
//            (key,resultvalues.map(_._2) ++ samplevalues)
//          }
//          else
//          {
//            val othervalues = input2.partitioner match {
//              case Some(p) =>
//                val index = p.getPartition(key)
//                val process = (it: Iterator[(K, W)]) => {
//                  val buf = new ArrayBuffer[W]
//                  for (pair <- it if pair._1 == key) {
//                    buf += pair._2
//                  }
//                  buf
//                } : Seq[W]
//                val res = self.context.runJob(input2, process, Array(index))
//                res(0).asInstanceOf[Seq[W]]
//              case None =>
//                self.filter(_._1 == key).map(_._2).collect().toSeq.asInstanceOf[Seq[W]]
//            } //if the original does not consist such a key, then the whole key-value pair maybe missing, therefore, need to rejoin
//            (key, samplevalues ++ othervalues)
//          }
//        }).toMap
//
//        val joinvalueswithkey = self.map(p => {
//          var values = allvaluesforjoin.getOrElse(p._1,p._2).asInstanceOf[Seq[W]]
//          if(!values.isEmpty) {
//            Some(values.map(q => {
//              (p._1, (p._2,q))
//            }))
//          }
//          else {
//            None
//          }
//        })
//
//        val ss = joinvalueswithkey.flatMap(p => p).flatMap(p => p)
//        new dpobject(ss,joinresult)
      }


}
