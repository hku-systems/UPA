/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off println
package edu.hku.dp.checker

import breeze.linalg.{DenseVector, Vector, squaredDistance}
import edu.hku.cs.dp.dpread_checker
import org.apache.spark.sql.SparkSession

/**
  * K-means clustering.
  *
  * This is an example implementation for learning how to use Spark. For more conventional use,
  * please refer to org.apache.spark.ml.clustering.KMeans.
  */
object SparkKMeansDP_checker {

  def parseVector(line: String, D: Int): Vector[Double] = {
    DenseVector(line.split(',').map(_.toDouble).take(D))
  }

  def closestPoint(p: Vector[Double], centers: Array[Vector[Double]]): Int = {
    var bestIndex = 0
    var closest = Double.PositiveInfinity

    for (i <- 0 until centers.length) {
      val tempDist = squaredDistance(p, centers(i))
      if (tempDist < closest) {
        closest = tempDist
        bestIndex = i
      }
    }

    bestIndex
  }

  def main(args: Array[String]) {

    if (args.length < 3) {
      System.err.println("Usage: SparkKMeans <file> <k> <convergeDist>")
      System.exit(1)
    }

    val input_size = args(0).split('.').last
    val spark = SparkSession
      .builder
      .appName("LR-" + input_size + "-" + args(5))
      .getOrCreate()
    val t1 = System.nanoTime

    val lines = new dpread_checker(spark.sparkContext.textFile(args(0)))
    val ITERATIONS = args(1).toInt
    val K = args(2).toInt
    val D = args(3).toInt
    val sr = args(5).toInt
    val b_D = spark.sparkContext.broadcast(D)
    val data = lines.mapDP(p => parseVector(p,b_D.value),sr)

    val r = scala.util.Random
    var kPoints =  Array.fill(K)(Vector.fill(D)(r.nextDouble))

    for (i <- 1 to ITERATIONS) {
      var tempDist = 1.0

      val closest = data.mapDP(p => (closestPoint(p, kPoints), (p, 1)))

      var newPoints = scala.collection.mutable.Map[Int,Vector[Double]]()

      for(j <- 0 until K) {
        val b_k = spark.sparkContext.broadcast(j)
        val new_centroid = closest.filterDP(p => p._1 == b_k.value)
        if(new_centroid.isEmptyDP()) {
          newPoints += (j -> kPoints(j))
        }else {
          val value = new_centroid.mapDP(_._2).reduceDP_vector((a,b) => (a._1 + b._1,a._2 + b._2))
          newPoints += (j -> value._1.map(p => p / value._2))
        }
      }

      for (newP <- newPoints) {
        kPoints(newP._1) = newP._2
      }
      //      println(s"Finished iteration (delta = $tempDist)")
    }
    val duration = (System.nanoTime - t1) / 1e9d
    println("Execution time: " + duration)
    //    kPoints.foreach(println)
    spark.stop()
  }
}
// scalastyle:on println