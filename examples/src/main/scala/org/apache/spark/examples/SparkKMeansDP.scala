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
package org.apache.spark.examples

import breeze.linalg.{squaredDistance, DenseVector, Vector}
import edu.hku.cs.dp.{dpobject, dpread, dpobjectKV}
import org.apache.spark.sql.SparkSession

/**
  * K-means clustering.
  *
  * This is an example implementation for learning how to use Spark. For more conventional use,
  * please refer to org.apache.spark.ml.clustering.KMeans.
  */
object SparkKMeansDP {

  def parseVector(line: String): Vector[Double] = {
    DenseVector(line.split(' ').map(_.toDouble))
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

  def showWarning() {
    System.err.println(
      """WARN: This is a naive implementation of KMeans Clustering and is given as an example!
        |Please use org.apache.spark.ml.clustering.KMeans
        |for more conventional use.
      """.stripMargin)
  }

  def main(args: Array[String]) {

    if (args.length < 3) {
      System.err.println("Usage: SparkKMeans <file> <k> <convergeDist>")
      System.exit(1)
    }

    showWarning()

    val spark = SparkSession
      .builder
      .appName("SparkKMeans")
      .getOrCreate()

    val lines = new dpread(spark.read.textFile(args(0)).rdd)
    val data = lines.mapDP(parseVector _).cache()
    val K = args(1).toInt
    val convergeDist = args(2).toDouble

    val kPoints = data.takeSample(withReplacement = false, K, 42)
    var tempDist = 1.0

    while(tempDist > convergeDist) {
      val closest = data.mapDPKV (p => (closestPoint(p, kPoints), (p, 1)))

      val pointStats = closest.reduceByKeyDP{case ((p1, c1), (p2, c2)) => (p1 + p2, c1 + c2)}
      //add noise to p1 + p2 & c1 + c2

      val newPointsDP = pointStats.mapDPKV{pair =>
        (pair._1, pair._2._1 * (1.0 / pair._2._2))}//Here should be 4 itms finding average

      val newPoints = newPointsDP.original.collectAsMap()
//      println("newPoints count: " + newPointsDP.original.count)
//      println("KeyOfNewPoints: ")
//      newPoints.map(pp => println(pp._1))
      //********************Add noise**************************
      val braodcastMap = spark.sparkContext.broadcast(newPoints)//why broadcast original? because sample has more element, can better leverage parallelism

      val newPointsSample = newPointsDP.sample.map(p => {
        var OriginalValue = braodcastMap.value.getOrElse(p._1,p._2)//broadcast orignal result to each sample
        val diff = OriginalValue.toArray.zip(p._2.toArray).map( q => {
             math.abs(q._1 - q._2)//First need to find the difference
          })
        (p._1,diff)
      })

      val newPointsSampleMaxLocalSensitivity = newPointsSample.reduceByKey((a,b) => {
        a.zip(b).map(m =>
        math.max(m._1,m._2))
      })
      println("newPointsSampleMaxLocalSensitivity count: " + newPointsSampleMaxLocalSensitivity.count)
      println("newPointsSampleMaxLocalSensitivity: ")
      newPointsSampleMaxLocalSensitivity.collect().foreach(n => {
        print(n._1 + ": ")
        n._2.foreach(nn => {
          print(nn + " ")
        })
        println("")
      })
      //*************End of adding noise**************************

      tempDist = 0.0
      for (i <- 0 until K) {
        tempDist += squaredDistance(kPoints(i), newPoints(i))//eg  this
        //tempDist += squaredDistance(kPoints(i), newPoints(i))
      }

      for (newP <- newPoints) {
        kPoints(newP._1) = newP._2
      }
      println("Finished iteration (delta = " + tempDist + ")")
    }

    println("Final centers:")
    kPoints.foreach(println)
    spark.stop()
  }
}
// scalastyle:on println
// scalastyle:on println

//In prior work (e.g., [2, 20, 22, 24]), the total number of iterations
//T is fixed a priori, and the desired privacy cost, say ϵ, is split across
//the iterations: ϵ = ϵ1 + · · · + ϵT . For any iteration t, the variance
//of Yt
//is a function of 1/ϵt and depends on which version of differential
//privacy is being used (e.g., pure differential privacy [8],
//approximate differential privacy [7], or zero-mean concentrated
//differential privacy [4]). Furthermore, in prior work, the privacy
//budget is evenly split across iterations, so ϵ1 = · · · = ϵT = ϵ/T .

