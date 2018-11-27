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

import java.util.Random

import scala.math.exp

import breeze.linalg.{DenseVector, Vector}

import org.apache.spark.sql.SparkSession
import edu.hku.cs.dp.{dpobject, dpread, dpobjectKV}
/**
  * Logistic regression based classification.
  *
  * This is an example implementation for learning how to use Spark. For more conventional use,
  * please refer to org.apache.spark.ml.classification.LogisticRegression.
  */
object SparkHdfsLRDP {
  val D = 2   // Number of dimensions
  val rand = new Random(42)

  case class DataPoint(x: Vector[Double], y: Double)

  def parsePoint(line: String): DataPoint = {
    val tok = new java.util.StringTokenizer(line, " ")
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
      System.err.println("Usage: SparkHdfsLRDP <file> <iters>")
      System.exit(1)
    }

    showWarning()

    val spark = SparkSession
      .builder
      .appName("SparkHdfsLRDP")
      .getOrCreate()

    val inputPath = args(0)
    val lines = new dpread(spark.read.textFile(inputPath).rdd)

    val points = lines.mapDP(parsePoint)

//    println("lines.mapDP(parsePoint): ")
//    points.original.collect().foreach(print)
//    println("End of lines.mapDP(parsePoint)")

    val ITERATIONS = args(1).toInt

    // Initialize w to a random value
    var w = DenseVector(0.2,0.6)
    println("Initial w: " + w)

    for (i <- 1 to ITERATIONS) {
      println("On iteration " + i)
      val gradientDP1 = points.mapDP { p =>
        p.x * (1 / (1 + exp(-p.y * (w.dot(p.x)))) - 1) * p.y
      }

//      println("gradientDP1.original.collect(): ")
//      gradientDP1.original.collect().foreach(print)
//      println("End of gradientDP1.original.collect()")
      val gradientDP = gradientDP1.reduceDP(_ + _)
      //************Add noise*****************

      //heristic: first collect original
      //then collect sample as sample has many items so can better use parallelization

      val gradient = gradientDP._2
      val broadcastOriginal = spark.sparkContext.broadcast(gradient)
      val originalSample = gradientDP._1.map(pp => {
        val originalWeighted = broadcastOriginal.value
        pp.toArray.zip(originalWeighted.toArray).map(ppp => {
          math.abs(ppp._2 - ppp._1)
        })
      }).reduce((a,b) => {
        a.zip(b).map(ab => {
          math.max(ab._1,ab._2)
        })
      })

      println("Local sensitivity")
      originalSample.foreach(osp => {
        print(osp + " ")
      })
      println("")

      //******End of add noise*****************
      w -= gradient
    }

    println("Final w: " + w)
    spark.stop()
  }
}