package edu.hku.cs.dp

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD

import scala.collection.immutable.HashMap
import scala.collection.immutable.HashSet
import scala.collection.mutable.ArrayBuffer
import scala.math.{exp, pow}
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
      val epsilon = 0.1
      val delta = pow(10,-8)
      val k_distance_double = 1/epsilon
      val k_distance = k_distance_double.toInt
      val beta = epsilon / (2*scala.math.log(2/delta))

      //parallelise inter key operation or intra key operation
      //seems use less collect is better
      //because collect then parallelise requires more rtt and sorting
      def reduceByKeyDP_deep(func: (V, V) => V): (Array[(K,Array[RDD[V]])],RDD[(K,V)]) = {
//reduce shuffle, use map rather than reduce/reduceByKey
        val originalresult = inputoriginal.reduceByKey(func)//Reduce original first
        val aggregatedResult = originalresult.union(sample).reduceByKey(func)
        val broadcast_result = original.sparkContext.broadcast(originalresult.collect().toMap)
        val broadcast_aggregated = original.sparkContext.broadcast(aggregatedResult.collect().toMap)

           //should do based on key to reduce shuffling, 1) we assume only few distinct keys
          //in the output, 2)the total output produced is less than 1111 because
          //inter keys differing element provides redundant information, so
          //we do not consider them
          //Indeed it is like reduceDP if we order the sample based on the keys,
          //just we do not consider inter-keys sampling
          val nighnouring_output = sample.groupByKey.collect().map(p => {
            var inner_num = 0
            var outer_num = k_distance
            val sample_count = p._2.size //e.g., 64
            val broadcast_sample = original.sparkContext.broadcast(p._2.toArray)
            if (sample_count <= 1) {
              val inner = new Array[RDD[V]](1) //directly return
              if(sample_count == 0)
                inner(0) = original.sparkContext.parallelize(aggregatedResult.lookup(p._1))
              else
                inner(0) = original.sparkContext.parallelize(originalresult.lookup(p._1)) //without that sample
              (p._1,inner)
            }
            else {
              if(sample_count <= k_distance - 1)
                outer_num = k_distance - 1
              else
                outer_num = k_distance //outer_num = 8
              var array = new Array[RDD[V]](outer_num)
              var i = outer_num
              while(i  > 0) {
                val up_to_index = (sample_count - i).toInt
                if(i == outer_num) {
                  //          println("sample_count: " + sample_count)
                  //          println("outer-most loop: " + up_to_index)
                  val inner_array = original.sparkContext.parallelize(0 to up_to_index - 1) //(0,1,2,3,4,5,6,7)
                    .map(q => {
                    val inside_array = broadcast_sample.value.patch(q, Nil, i)
                    func(inside_array.reduce(func),Seq(broadcast_result.value.get(p._1)).flatMap(l => l).head) //(0 -> 7, 8 -> 15, 16, 24, 32, 40, 48, 56)
                  })
                  println("i is " + i)
                  inner_array.collect().foreach(println)
                  array(i - 1) = inner_array
                } else {
                  val array_collected = array(i).collect()
                  val upper_array = original.sparkContext.broadcast(array_collected)

                  val array_length = array_collected.length
                  val array_length_broadcast = original.sparkContext.broadcast(array_length)
                  val up_to_index = (sample_count - i).toInt

                  //          println("sample_count: " + sample_count)
                  //          println("array_length: " + array_length)
                  //          println("current i: " + i)
                  //          println("outer_num: " + outer_num)
                  //          println("up_to_index: " + up_to_index)

                  val inner_array = original.sparkContext.parallelize(0 to up_to_index - 1) //(0,1,2,3,4,5,6,7)
                    .map(q => {
                    if(q < array_length_broadcast.value) {
                      func(upper_array.value(q), broadcast_sample.value(q + i + 1))//no need to include result, as it is included
                    }
                    else if(p == array_length_broadcast)
                      func(upper_array.value(q - 1),broadcast_sample.value(q))
                    else {
                      val inside_array = broadcast_sample.value.patch(q, Nil, i)
                      func(inside_array.reduce(func),Seq(broadcast_result.value.get(p._1)).flatMap(l => l).head)
                    }
                  })
                  println("i is " + i)
                  inner_array.collect().foreach(println)
                  array(i - 1) = inner_array
                }
                i = i - 1
              }
              (p._1,array)
            }
          })

          (nighnouring_output,aggregatedResult)
        }


      def reduceByKeyDP(func: (V, V) => V): RDD[(K,V)] = {
        val array = reduceByKeyDP_deep(func)
        val ls = array._1.map(p => {
          val lls = p._2.map(q => {

            val sensitivity = q match {
              case qint: RDD[Int] =>
                val max = qint.asInstanceOf[RDD[Int]].max
                val min = qint.asInstanceOf[RDD[Int]].min
                val value_of_key = array._2.lookup(p._1)(0).asInstanceOf[Int]
                scala.math.max(scala.math.abs(max - value_of_key),scala.math.abs(min - value_of_key))
              case qdouble: RDD[Double] =>
                val max = qdouble.asInstanceOf[RDD[Double]].max
                val min = qdouble.asInstanceOf[RDD[Double]].min
                val value_of_key = array._2.lookup(p._1)(0).asInstanceOf[Double]
                scala.math.max(scala.math.abs(max - value_of_key),scala.math.abs(min - value_of_key))
              case qtuples: RDD[(Double,Double,Double,Double,Double,Int)]=>
                val result = array._2.asInstanceOf[RDD[(K,(Double,Double,Double,Double,Double,Int))]]
                val this_key = result.lookup(p._1)(0)
                val result_broadcast = result.sparkContext.broadcast(this_key)
                val max = qtuples
                  .asInstanceOf[RDD[(Double,Double,Double,Double,Double,Int)]]
                  .reduce((a,b) => {
                    val b_result = result_broadcast.value
                    val first = scala.math.max(scala.math.abs(a._1 - b_result._1),scala.math.abs(b._1 - b_result._1))
                    val second = scala.math.max(scala.math.abs(a._2 - b_result._2),scala.math.abs(b._2 - b_result._2))
                    val third = scala.math.max(scala.math.abs(a._3 - b_result._3),scala.math.abs(b._3 - b_result._3))
                    val forth = scala.math.max(scala.math.abs(a._4 - b_result._4),scala.math.abs(b._4 - b_result._4))
                    val fifth = scala.math.max(scala.math.abs(a._5 - b_result._5),scala.math.abs(b._5 - b_result._5))
                    val sixth = scala.math.max(scala.math.abs(a._6 - b_result._6),scala.math.abs(b._6 - b_result._6))
                    (first, second, third, forth,fifth, sixth)
                  })
              case _ => throw new Exception("Cannot match any pattern")

            }
            sensitivity.asInstanceOf[V]
          }).asInstanceOf[Any]

          val final_senstiviity = lls match {
            case intArray: Array[Int] =>
              var intArray_in = intArray.asInstanceOf[Array[Int]]
              var max_nls = 0
              for (i <- 0 until intArray_in.length) {
                intArray_in(i) = intArray_in(i)*exp(-beta*(i+1)).toInt
                if(intArray_in(i) > max_nls)
                  max_nls = intArray_in(i)
              }
              max_nls
            case doubleArray: Array[Double] =>
              var doubleArray_in = doubleArray.asInstanceOf[Array[Double]]
              var max_nls = 0.0
              for (i <- 0 until doubleArray_in.length) {
                doubleArray_in(i) = doubleArray_in(i)*exp(-beta*(i+1))
                if(doubleArray_in(i) > max_nls)
                  max_nls = doubleArray_in(i)
              }
              max_nls
            case tupleArray: Array[(Double,Double,Double,Double,Double,Int)] =>
              var tupleArray_in = tupleArray.asInstanceOf[Array[(Double,Double,Double,Double,Double,Int)]]
              var max_nls_1 = 0.0
              var max_nls_2 = 0.0
              var max_nls_3 = 0.0
              var max_nls_4 = 0.0
              var max_nls_5 = 0.0
              var max_nls_6 = 0
              for (i <- 0 until tupleArray_in.length) {
                if(tupleArray_in(i)._1*exp(-beta*(i+1)) > max_nls_1)
                  max_nls_1 = tupleArray_in(i)._1
                if(tupleArray_in(i)._2*exp(-beta*(i+1)) > max_nls_2)
                  max_nls_2 = tupleArray_in(i)._2
                if(tupleArray_in(i)._3*exp(-beta*(i+1)) > max_nls_3)
                  max_nls_3 = tupleArray_in(i)._3
                if(tupleArray_in(i)._4*exp(-beta*(i+1)) > max_nls_4)
                  max_nls_4 = tupleArray_in(i)._4
                if(tupleArray_in(i)._5*exp(-beta*(i+1)) > max_nls_5)
                  max_nls_5 = tupleArray_in(i)._5
                if(tupleArray_in(i)._6*exp(-beta*(i+1)).toInt > max_nls_6)
                  max_nls_6 = tupleArray_in(i)._6
              }
              (max_nls_1,max_nls_2,max_nls_3,max_nls_4,max_nls_5,max_nls_6)
          }


          (p._1,final_senstiviity.asInstanceOf[V])
        })
        println("sensitivity is: ")
        ls.foreach(println)
        array._2
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

      }


}
