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
package org.apache.spark.mllib.stat

import org.apache.commons.math3.distribution.TDistribution
import org.apache.commons.math3.util.FastMath
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
/**
  * Created by yuhao on 12/31/15.
  */
object ttest {
  def tTest(m1: Double, m2: Double, s1: Double, s2: Double, n1: Int, n2: Int): Double = {
    val t: Double = math.abs((m1 - m2) / FastMath.sqrt((s1 / n1) + (s2 / n2)))

    // pass a null rng to avoid unneeded overhead as we will not sample from this distribution
    val distribution: TDistribution = new TDistribution(null, 2.0)
    2.0 * distribution.cumulativeProbability(-t)
  }

  def main(args: Array[String]): Unit = {
    // this is used to implicitly convert an RDD to a DataFrame.
    val sparkConf = new SparkConf().setAppName("AOL Query")
    val sc = new SparkContext(sparkConf)
  }
}