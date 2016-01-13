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

package org.apache.spark.mllib.evaluation

import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.apache.spark.{SparkConf, SparkContext}

class MyBinaryClassificationMetricsSuite extends FunSuite with BeforeAndAfterAll {

  @transient var sc: SparkContext = _

  override def beforeAll() {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("test")
    sc = new SparkContext(conf)
    super.beforeAll()
  }

  override def afterAll() {
    if (sc != null) {
      sc.stop()
    }
    super.afterAll()
  }

  private def areWithinEpsilon(x: (Double, Double)): Boolean = Math.abs (x._1 - x._2) <= 1E-5

  private def pairsWithinEpsilon(x: ((Double, Double), (Double, Double))): Boolean =
    (Math.abs (x._1._1 - x._2._1) <= 1E-5) && (Math.abs (x._1._2 - x._2._2) <= 1E-5)

  private def pairPairsWithinEpsilon(x: (((Double, Double), (Double, Long)), ((Double, Double),
      (Double, Long)))): Boolean =
    (Math.abs (x._1._1._1 - x._2._1._1) <= 1E-5) && (Math.abs (x._1._1._2 - x._2._1._2) <= 1E-5) &&
      (Math.abs (x._1._2._1 - x._2._2._1) <= 1E-5) && x._1._2._2 == x._2._2._2

  private def assertSequencesMatch(left: Seq[Double], right: Seq[Double]): Unit = {
      assert(left.zip(right).forall(areWithinEpsilon))
  }

  private def assertTupleSequencesMatch(left: Seq[(Double, Double)],
       right: Seq[(Double, Double)]): Unit = {
    assert(left.zip(right).forall(pairsWithinEpsilon))
  }

  private def assertTupleTupleSequencesMatch(left: Seq[((Double, Double), (Double, Long))],
       right: Seq[((Double, Double), (Double, Long))]): Unit = {
    assert(left.zip(right).forall(pairPairsWithinEpsilon))
  }

  private def validateMetrics(metrics: MyBinaryClassificationMetrics,
      expectedCalibration: Seq[((Double, Double), (Double, Long))]) = {

    assertTupleTupleSequencesMatch(metrics.calibration().collect(), expectedCalibration)
  }

  test("binary evaluation metrics") {
    val scoreAndLabels = sc.parallelize(
      Seq((0.1, 0.0), (0.1, 1.0), (0.4, 0.0), (0.6, 0.0), (0.6, 1.0), (0.6, 1.0), (0.8, 1.0)), 2)
    val metrics = new MyBinaryClassificationMetrics(scoreAndLabels)
    val calibration = Seq(((0.1, 0.1), (0.5, 2L)), ((0.4, 0.4), (0.0, 1L)), ((0.6, 0.6),
                           (2/3.0, 3L)), ((0.8, 0.8), (1.0, 1L)))

    validateMetrics(metrics, calibration)
  }

  test("binary evaluation metrics for RDD where all examples have positive label") {
    val scoreAndLabels = sc.parallelize(Seq((0.5, 1.0), (0.5, 1.0)), 2)
    val metrics = new MyBinaryClassificationMetrics(scoreAndLabels)

    val calibration = Seq(((0.5, 0.5), (1.0, 2L)))

    validateMetrics(metrics, calibration)
  }

  test("binary evaluation metrics for RDD where all examples have negative label") {
    val scoreAndLabels = sc.parallelize(Seq((0.5, 0.0), (0.5, 0.0)), 2)
    val metrics = new MyBinaryClassificationMetrics(scoreAndLabels)

    val calibration = Seq(((0.5, 0.5), (0.0, 2L)))

    validateMetrics(metrics, calibration)
  }

  test("binary evaluation metrics with downsampling") {
    val scoreAndLabels = Seq(
      (0.1, 0.0), (0.2, 0.0), (0.3, 1.0), (0.4, 0.0), (0.5, 0.0),
      (0.6, 1.0), (0.7, 1.0), (0.8, 0.0), (0.9, 1.0))

    val scoreAndLabelsRDD = sc.parallelize(scoreAndLabels, 1)

    val numBins = 4
    val downsampled = new MyBinaryClassificationMetrics(scoreAndLabelsRDD, numBins)
    val original = new MyBinaryClassificationMetrics(scoreAndLabelsRDD)
    val calibration = Array(((0.1, 0.3), (1/3.0, 3L)), ((0.4, 0.6), (1/3.0, 3L)), ((0.7, 0.9),
                             (2/3.0, 3L)))
    assertTupleTupleSequencesMatch(calibration, downsampled.calibration().collect())
  }

}
