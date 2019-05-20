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
package edu.hku.dp.original

import java.util.Random

import breeze.linalg.{DenseVector, Vector}
import org.apache.spark.sql.SparkSession

import scala.math.exp

object SimpleSVM {
  val D = 2   // Number of dimensions
  val rand = new Random(42)

  case class DataPoint(x: Vector[Double], y: Double)

  def parsePoint(line: String): DataPoint = {
    val tok = new java.util.StringTokenizer(line, ",")
    var y = tok.nextToken.toDouble
    var x = new Array[Double](D)
    var i = 0
    while (i < D) {
      x(i) = tok.nextToken.toDouble; i += 1
    }
    DataPoint(new DenseVector(x), y)
  }

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder
      .appName("Simple SVM")
      .getOrCreate()

    val inputPath = args(0)
    val lines = spark.read.textFile(inputPath).rdd

    val points = lines.map(parsePoint)
    val ITERATIONS = args(1).toInt

    // Initialize w to a random value
    var dual_coef = DenseVector(0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0)
//    println("Initial w: " + w)
    //      println("On iteration " + i)
      val dual_coef_error = points.map { p =>
        val classifier = p.x * (1 / (1 + exp(-p.y * (w.dot(p.x)))) - 1) * p.y
        (p.y, p.y - classifier.dot(p.x))
      }
      val margin_norm = dual_coef_error.reduceByKey((a,b) => scala.math.min(a,b)).map(p => p._2).mean

    println(margin_norm)
    spark.stop()
  }
}
// scalastyle:on println
