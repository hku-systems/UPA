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

import java.util.Random

import breeze.linalg.{DenseVector, Vector}
import edu.hku.cs.dp.dpread_checker
import org.apache.spark.sql.SparkSession

import scala.math.exp

/**
  * Logistic regression based classification.
  *
  * This is an example implementation for learning how to use Spark. For more conventional use,
  * please refer to org.apache.spark.ml.classification.LogisticRegression.
  */
object SparkHdfsLRDP_checker {
  val rand = new Random(42)

  case class DataPoint(x: Vector[Double], y: Double)

  def parsePoint(line: String, D: Int): DataPoint = {
    val tok = new java.util.StringTokenizer(line, ",")
    var y = tok.nextToken.toDouble
    var x = new Array[Double](D)
    var i = 0
    while (i < D) {
      x(i) = tok.nextToken.toDouble; i += 1
    }
    DataPoint(new DenseVector(x), y)
  }

  def showWarning() {
    System.err.println(
      """WARN: This is a naive implementation of Logistic Regression and is given as an example!
        |Please use org.apache.spark.ml.classification.LogisticRegression
        |for more conventional use.
      """.stripMargin)
  }

  def main(args: Array[String]) {

    if (args.length < 2) {
      System.err.println("Usage: SparkHdfsLR <file> <iters>")
      System.exit(1)
    }

    showWarning()

    val input_size = args(0).split('.').last

    val spark = SparkSession
      .builder
      .appName("LR-" + input_size + "-" + args(4))
      .getOrCreate()

    val t1 = System.nanoTime
    val lines = new dpread_checker(spark.sparkContext.textFile(args(0)))

    val ITERATIONS = args(1).toInt
    val D1 = args(2).toInt
    val sr = args(4).toInt
    val points = lines.mapDP(p => parsePoint(p,D1),sr)

    // Initialize w to a random value
    val r = scala.util.Random
    var w = DenseVector.fill(D1)(r.nextDouble)
    //    println("Initial w: " + w)

    for (i <- 1 to ITERATIONS) {
      val gradient = points.mapDP { p =>
        p.x * (1 / (1 + exp(-p.y * (w.dot(p.x)))) - 1) * p.y
      }.reduceDP_vector((a,b) => a + b)
      w -= gradient
    }

    val duration = (System.nanoTime - t1) / 1e9d
    println("Execution time: " + duration)
    spark.stop()
  }
}
// scalastyle:on println
