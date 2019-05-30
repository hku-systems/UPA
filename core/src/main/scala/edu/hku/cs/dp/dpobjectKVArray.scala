package edu.hku.cs.dp

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD

import scala.math.{exp, pow}
import scala.reflect.ClassTag

/**
  * Created by lionon on 10/28/18.
  */
class dpobjectKVArray[K, V](var inputsample: Array[RDD[(K, V)]], var inputsample_advance: Array[RDD[(K, V)]], var inputoriginal: RDD[(K, V)])
                           (implicit kt: ClassTag[K], vt: ClassTag[V], ord: Ordering[K] = null)
  extends Logging with Serializable
    {
      var sample = inputsample
      var sample_advance = inputsample_advance
      var original = inputoriginal
      val epsilon = 0.1
      val delta = pow(10,-8)
      val k_distance_double = 1/epsilon
      val k_distance = k_distance_double.toInt
      val beta = epsilon / (2*scala.math.log(2/delta))


      def filterDPKV(f: ((K,V)) => Boolean) : dpobjectKVArray[K, V] = {
      val t1 = System.nanoTime
        val r1 = inputsample.map(p => p.filter(f))
        val r2 = inputsample_advance.map(p => p.filter(f))
        val r3 = inputoriginal.filter(f)
      val duration = (System.nanoTime - t1) / 1e9d
      println("filterDP: " + duration)
        new dpobjectKVArray(r1,r2,r3)
      }
      //********************Join****************************


      def joinDP[W](otherDP: RDD[(K, W)]): dpobjectArray[(K, (V, W))] = {

        //No need to care about sample2 join sample1
        val t1 = System.nanoTime
        val joinresult = original.join(otherDP)

        //        val advance_original = sample_advance.map(q => {
        //          q.join(otherDP)
        //        })

        val advance_original_tmp = sample_advance
          .filter(p => !p.isEmpty())
          .zipWithIndex
          .map(p => {
            val index = original.sparkContext.broadcast(p._2)
            p._1.map(q => (q._1,(q._2,index.value))).collect()
          }).flatten

        val advance_original = original.sparkContext.parallelize(advance_original_tmp)
          .join(otherDP)
          .map(p => (p._2._1._2,(p._1,(p._2._1._1,p._2._2))))
          .groupByKey()
          .collect()
          .map(p => original.sparkContext.parallelize(p._2.toSeq))

        //        val with_sample = sample.map(q => {
        //          q.join(otherDP)
        //        })

        val sample_tmp = sample
          .filter(p => !p.isEmpty())
          .zipWithIndex
          .map(p => {
            val index = original.sparkContext.broadcast(p._2)
            p._1.map(q => (q._1,(q._2,index.value))).collect()
          }).flatten

        val with_sample = original.sparkContext.parallelize(sample_tmp)
          .join(otherDP)
          .map(p => (p._2._1._2,(p._1,(p._2._1._1,p._2._2))))
          .groupByKey()
          .collect()
          .map(p => original.sparkContext.parallelize(p._2.toSeq))

  val duration = (System.nanoTime - t1) / 1e9d
  println("JoinDP: " + duration)
        new dpobjectArray(with_sample,advance_original,joinresult)
      }


      def joinDP[W](otherDP: dpobjectKV[K, W]): dpobjectArray[(K, (V, W))] = {

        //No need to care about sample2 join sample1
  val t1 = System.nanoTime
        val input2 = otherDP.original
        val input2_sample = otherDP.original
        val joinresult = original.join(otherDP.original)

        val zipin_advance = otherDP.sample_advance
          .zipWithIndex()
          .map(p => (p._1._1,(p._1._2,p._2)))
        val original_advance = original
          .join(zipin_advance)
          .map(p => (p._2._2._2,(p._1,(p._2._1,p._2._2._1))))
          .groupByKey()
          .collect()
          .map(p => original.sparkContext.parallelize(p._2.toSeq))

        //        val advance_original = sample_advance.map(q => {
        //          q.join(otherDP.original)
        //        })

        //don't just iterate through array and join, too time consuming
        val advance_original_tmp = sample_advance
          .filter(p => !p.isEmpty())
          .zipWithIndex
          .map(p => {
            val index = original.sparkContext.broadcast(p._2)
            p._1.map(q => (q._1,(q._2,index.value))).collect()
          }).flatten

        val advance_original = original.sparkContext.parallelize(advance_original_tmp)
          .join(otherDP.original)
          .map(p => (p._2._1._2,(p._1,(p._2._1._1,p._2._2))))
          .groupByKey()
          .collect()
          .map(p => original.sparkContext.parallelize(p._2.toSeq))

        //        val advance_advance = sample_advance.join(otherDP.sample_advance)
        val zipin = otherDP.sample
          .zipWithIndex()
          .map(p => (p._1._1,(p._1._2,p._2)))
        val with_input2_sample = original
          .join(zipin)
          .map(p => (p._2._2._2,(p._1,(p._2._1,p._2._2._1))))
          .groupByKey()
          .collect()
          .map(p => original.sparkContext.parallelize(p._2.toSeq))

        //        val with_sample = sample.map(q => {
        //          q.join(otherDP.original)
        //        })

        val sample_tmp = sample
          .filter(p => !p.isEmpty())
          .zipWithIndex
          .map(p => {
            val index = original.sparkContext.broadcast(p._2)
            p._1.map(q => (q._1,(q._2,index.value))).collect()
          }).flatten

        val with_sample = original.sparkContext.parallelize(sample_tmp)
          .join(otherDP.original)
          .map(p => (p._2._1._2,(p._1,(p._2._1._1,p._2._2))))
          .groupByKey()
          .collect()
          .map(p => original.sparkContext.parallelize(p._2.toSeq))
  val duration = (System.nanoTime - t1) / 1e9d
  println("JoinDP: " + duration)
        new dpobjectArray(with_input2_sample ++ with_sample,original_advance ++ advance_original,joinresult)
      }

      def joinDP[W](otherDP: dpobjectKVArray[K, W]): dpobjectArray[(K, (V, W))] = {

        //No need to care about sample2 join sample1
      val t1 = System.nanoTime
        val input2 = otherDP.original
        val input2_sample = otherDP.original
        val joinresult = original.join(otherDP.original)

        //        val original_advance = otherDP.sample_advance.map(q => {
        //          original.join(q)
        //        })

        val original_advance_tmp = otherDP.sample_advance
          .filter(p => !p.isEmpty())
          .zipWithIndex
          .map(p => {
            val index = original.sparkContext.broadcast(p._2)
            p._1.map(q => (q._1,(q._2,index.value))).collect()
          }).flatten

        val original_advance_p = original.sparkContext.parallelize(original_advance_tmp)

        val original_advance = original.join(original_advance_p)
          .map(p => (p._2._2._2,(p._1,(p._2._1,p._2._2._1))))
          .groupByKey()
          .collect()
          .map(p => original.sparkContext.parallelize(p._2.toSeq))

        //This is a dpobjectArray, so can manually add index
        val advance_original_tmp = sample_advance
          .filter(p => !p.isEmpty())
          .zipWithIndex
          .map(p => {
            val index = original.sparkContext.broadcast(p._2)
            p._1.map(q => (q._1,(q._2,index.value))).collect()
          }).flatten

        val advance_original = original.sparkContext.parallelize(advance_original_tmp)
          .join(otherDP.original)
          .map(p => (p._2._1._2,(p._1,(p._2._1._1,p._2._2))))
          .groupByKey()
          .collect()
          .map(p => original.sparkContext.parallelize(p._2.toSeq))

        //        val with_input2_sample = otherDP.sample.map(q => {
        //          original.join(q)
        //        })

        val with_input_tmp = otherDP.sample
          .filter(p => !p.isEmpty())
          .zipWithIndex
          .map(p => {
            val index = original.sparkContext.broadcast(p._2)
            p._1.map(q => (q._1,(q._2,index.value))).collect()
          }).flatten

        val with_input_p = original.sparkContext.parallelize(with_input_tmp)

        val with_input2_sample = original.join(with_input_p)
          .map(p => (p._2._2._2,(p._1,(p._2._1,p._2._2._1))))
          .groupByKey()
          .collect()
          .map(p => original.sparkContext.parallelize(p._2.toSeq))

        val sample_tmp = sample
          .filter(p => !p.isEmpty())
          .zipWithIndex
          .map(p => {
            val index = original.sparkContext.broadcast(p._2)
            p._1.map(q => (q._1,(q._2,index.value))).collect()
          }).flatten

        val with_sample = original.sparkContext.parallelize(sample_tmp)
          .join(otherDP.original)
          .map(p => (p._2._1._2,(p._1,(p._2._1._1,p._2._2))))
          .groupByKey()
          .collect()
          .map(p => original.sparkContext.parallelize(p._2.toSeq))
  val duration = (System.nanoTime - t1) / 1e9d
  println("JoinDP: " + duration)
        new dpobjectArray(with_input2_sample ++ with_sample,original_advance ++ advance_original,joinresult)
      }

}
