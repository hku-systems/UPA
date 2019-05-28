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
package edu.hku.dp

import breeze.linalg.{DenseVector, Vector, squaredDistance}
import edu.hku.cs.dp.dpread
import org.apache.spark.sql.SparkSession

/**
  * K-means clustering.
  *
  * This is an example implementation for learning how to use Spark. For more conventional use,
  * please refer to org.apache.spark.ml.clustering.KMeans.
  */
object SparkKMeansDP {

  def parseVector(line: String): Vector[Double] = {
    DenseVector(line.split(',').map(_.toDouble))
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

    val spark = SparkSession
      .builder
      .appName("SparkKMeans")
      .getOrCreate()

    val lines = new dpread(spark.sparkContext.textFile(args(0)),spark.sparkContext.textFile(args(1)))
    val data = lines.mapDP(parseVector _)
    val K = 5

    val kPoints = Array(Vector(0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1),
      Vector(0.2,0.2,0.2,0.2,0.2,0.2,0.2,0.2,0.2,0.2,0.2),
      Vector(0.3,0.3,0.3,0.3,0.3,0.3,0.3,0.3,0.3,0.3,0.3),
      Vector(0.4,0.4,0.4,0.4,0.4,0.4,0.4,0.4,0.4,0.4,0.4),
      Vector(0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5))

    //    while(tempDist > convergeDist) {
    val closest = data.mapDPKV(p => (closestPoint(p, kPoints),1.0))
    val pointStats = closest.reduceByKeyDP_Double((a,b) => a + b)

    println("closest: " + pointStats)
    spark.stop()
  }
}
// scalastyle:on println
