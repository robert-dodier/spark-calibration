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

import org.apache.spark.annotation.Since
import org.apache.spark.Logging
import org.apache.spark.mllib.evaluation.binary._
import org.apache.spark.rdd.{RDD, UnionRDD}
import org.apache.spark.sql.DataFrame

/**
 * Evaluator for binary classification.
 *
 * @param scoreAndLabels an RDD of (score, label) pairs.
 * @param numBins if greater than 0, then the curves (ROC curve, PR curve, calibration curve)
 *                computed internally will be down-sampled to this many "bins". If 0, no
 *                down-sampling will occur. This is useful because the curve contains a point for
 *                each distinct score in the input, and this could be as large as the input itself
 *                -- millions of points or more, when thousands may be entirely sufficient to
 *                summarize the curve. After down-sampling, the curves will instead be made of
 *                approximately `numBins` points instead. Points are made from bins of equal
 *                numbers of consecutive points. The size of each bin is
 *                `floor(scoreAndLabels.count() / numBins)`, which means the resulting number
 *                of bins may not exactly equal numBins. The last bin in each partition may
 *                be smaller as a result, meaning there may be an extra sample at
 *                partition boundaries.
 */
@Since("1.0.0")
class MyBinaryClassificationMetrics @Since("1.3.0") (
    @Since("1.3.0") val scoreAndLabels: RDD[(Double, Double)],
    @Since("1.3.0") val numBins: Int) extends Logging {

  require(numBins >= 0, "numBins must be nonnegative")

  /**
   * Defaults `numBins` to 0.
   */
  @Since("1.0.0")
  def this(scoreAndLabels: RDD[(Double, Double)]) = this(scoreAndLabels, 0)

  /**
   * An auxiliary constructor taking a DataFrame.
   * @param scoreAndLabels a DataFrame with two double columns: score and label
   */
  private[mllib] def this(scoreAndLabels: DataFrame) =
    this(scoreAndLabels.map(r => (r.getDouble(0), r.getDouble(1))))

  /**
   * Unpersist intermediate RDDs used in the computation.
   */
  @Since("1.0.0")
  def unpersist() {
    cumulativeCounts.unpersist()
  }

  /**
   * Returns thresholds in descending order.
   */
  @Since("1.0.0")
  def thresholds(): RDD[Double] = cumulativeCounts.map(_._1)

  /**
   * Returns the receiver operating characteristic (ROC) curve,
   * which is an RDD of (false positive rate, true positive rate)
   * with (0.0, 0.0) prepended and (1.0, 1.0) appended to it.
   * @see http://en.wikipedia.org/wiki/Receiver_operating_characteristic
   */
  @Since("1.0.0")
  def roc(): RDD[(Double, Double)] = {
    val rocCurve = createCurve(FalsePositiveRate, Recall)
    val sc = confusions.context
    val first = sc.makeRDD(Seq((0.0, 0.0)), 1)
    val last = sc.makeRDD(Seq((1.0, 1.0)), 1)
    new UnionRDD[(Double, Double)](sc, Seq(first, rocCurve, last))
  }

  /**
   * Computes the area under the receiver operating characteristic (ROC) curve.
   */
  @Since("1.0.0")
  def areaUnderROC(): Double = AreaUnderCurve.of(roc())

  /**
   * Returns the precision-recall curve, which is an RDD of (recall, precision),
   * NOT (precision, recall), with (0.0, 1.0) prepended to it.
   * @see http://en.wikipedia.org/wiki/Precision_and_recall
   */
  @Since("1.0.0")
  def pr(): RDD[(Double, Double)] = {
    val prCurve = createCurve(Recall, Precision)
    val sc = confusions.context
    val first = sc.makeRDD(Seq((0.0, 1.0)), 1)
    first.union(prCurve)
  }

  /**
   * Computes the area under the precision-recall curve.
   */
  @Since("1.0.0")
  def areaUnderPR(): Double = AreaUnderCurve.of(pr())

  /**
   * Returns the (threshold, F-Measure) curve.
   * @param beta the beta factor in F-Measure computation.
   * @return an RDD of (threshold, F-Measure) pairs.
   * @see http://en.wikipedia.org/wiki/F1_score
   */
  @Since("1.0.0")
  def fMeasureByThreshold(beta: Double): RDD[(Double, Double)] = createCurve(FMeasure(beta))

  /**
   * Returns the (threshold, F-Measure) curve with beta = 1.0.
   */
  @Since("1.0.0")
  def fMeasureByThreshold(): RDD[(Double, Double)] = fMeasureByThreshold(1.0)

  /**
   * Returns the (threshold, precision) curve.
   */
  @Since("1.0.0")
  def precisionByThreshold(): RDD[(Double, Double)] = createCurve(Precision)

  /**
   * Returns the (threshold, recall) curve.
   */
  @Since("1.0.0")
  def recallByThreshold(): RDD[(Double, Double)] = createCurve(Recall)

  private lazy val (
    cumulativeCounts: RDD[(Double, BinaryLabelCounter)],
    confusions: RDD[(Double, BinaryConfusionMatrix)]) = {
    // Create a bin for each distinct score value, count positives and negatives within each bin,
    // and then sort by score values in descending order.
    val counts = scoreAndLabels.combineByKey(
      createCombiner = (label: Double) => new BinaryLabelCounter(0L, 0L) += label,
      mergeValue = (c: BinaryLabelCounter, label: Double) => c += label,
      mergeCombiners = (c1: BinaryLabelCounter, c2: BinaryLabelCounter) => c1 += c2
    ).sortByKey(ascending = false)

    val binnedCounts =
      // Only down-sample if bins is > 0
      if (numBins == 0) {
        // Use original directly
        counts
      } else {
        val countsSize = counts.count()
        // Group the iterator into chunks of about countsSize / numBins points,
        // so that the resulting number of bins is about numBins
        var grouping = countsSize / numBins
        if (grouping < 2) {
          // numBins was more than half of the size; no real point in down-sampling to bins
          logInfo(s"Curve is too small ($countsSize) for $numBins bins to be useful")
          counts
        } else {
          if (grouping >= Int.MaxValue) {
            logWarning(
              s"Curve too large ($countsSize) for $numBins bins; capping at ${Int.MaxValue}")
            grouping = Int.MaxValue
          }
          counts.mapPartitions(_.grouped(grouping.toInt).map { pairs =>
            // The score of the combined point will be just the first one's score
            val firstScore = pairs.head._1
            // The point will contain all counts in this chunk
            val agg = new BinaryLabelCounter()
            pairs.foreach(pair => agg += pair._2)
            (firstScore, agg)
          })
        }
      }

    val agg = binnedCounts.values.mapPartitions { iter =>
      val agg = new BinaryLabelCounter()
      iter.foreach(agg += _)
      Iterator(agg)
    }.collect()
    val partitionwiseCumulativeCounts =
      agg.scanLeft(new BinaryLabelCounter())(
        (agg: BinaryLabelCounter, c: BinaryLabelCounter) => agg.clone() += c)
    val totalCount = partitionwiseCumulativeCounts.last
    logInfo(s"Total counts: $totalCount")
    val cumulativeCounts = binnedCounts.mapPartitionsWithIndex(
      (index: Int, iter: Iterator[(Double, BinaryLabelCounter)]) => {
        val cumCount = partitionwiseCumulativeCounts(index)
        iter.map { case (score, c) =>
          cumCount += c
          (score, cumCount.clone())
        }
      }, preservesPartitioning = true)
    cumulativeCounts.persist()
    val confusions = cumulativeCounts.map { case (score, cumCount) =>
      (score, BinaryConfusionMatrixImpl(cumCount, totalCount).asInstanceOf[BinaryConfusionMatrix])
    }
    (cumulativeCounts, confusions)
  }

  /** Creates a curve of (threshold, metric). */
  private def createCurve(y: BinaryClassificationMetricComputer): RDD[(Double, Double)] = {
    confusions.map { case (s, c) =>
      (s, y(c))
    }
  }

  /** Creates a curve of (metricX, metricY). */
  private def createCurve(
      x: BinaryClassificationMetricComputer,
      y: BinaryClassificationMetricComputer): RDD[(Double, Double)] = {
    confusions.map { case (_, c) =>
      (x(c), y(c))
    }
  }

  /**
   * Returns the calibration or reliability curve,
   * which is represented as an RDD of `((s_min, s_max), (p, n))`
   * where `s_min` is the least score, `s_max` is the greatest score,
   * `p` is the fraction of positive examples, and `n` is the number of scores,
   * for each bin.
   *
   * `RDD.count` returns the actual number of bins, which might or might not
   * be the same as `numBins`.
   *
   * When `numBins` is zero, scores are not grouped;
   * in effect, each score is put into its own distinct bin.
   *
   * When `numBins` is greater than (number of distinct scores)/2,
   * `numBins` is ignored and scores are not grouped
   * (same as `numBins` equal to zero).
   *
   * When `distinctScoresCount/numBins` (rounded up) is greater than or
   * equal to `Int.MaxValue`, the actual number of bins is `distinctScoresCount/Int.MaxValue`
   * (rounded up).
   *
   * Otherwise, the actual number of bins is equal to `numBins`.
   *
   * @see Wikipedia article on calibration in classification:
   * [[http://en.wikipedia.org/wiki/Calibration_%28statistics%29#In_classification Link to article]]
   *
   * @see Mahdi Pakdaman Naeini, Gregory F. Cooper, Milos Hauskrecht.
   * Binary Classifier Calibration: Non-parametric approach.
   * [[http://arxiv.org/abs/1401.3390 Link to paper]]
   *
   * @see Alexandru Niculescu-Mizil, Rich Caruana.
   * Predicting Good Probabilities With Supervised Learning.
   * Appearing in Proceedings of the 22nd International Conference on Machine Learning,
   * Bonn, Germany, 2005.
   * [[http://www.cs.cornell.edu/~alexn/papers/calibration.icml05.crc.rev3.pdf Link to paper]]
   *
   * @see Properties and benefits of calibrated classifiers.
   * Ira Cohen, Moises Goldszmidt.
   * [[http://www.hpl.hp.com/techreports/2004/HPL-2004-22R1.pdf Link to paper]]
   */
  def calibration(): RDD[((Double, Double), (Double, Long))] = {
    assessedCalibration
  }

  private lazy val assessedCalibration: RDD[((Double, Double), (Double, Long))] = {
    val distinctScoresAndLabelCounts = scoreAndLabels.combineByKey(
      createCombiner = (label: Double) => new BinaryLabelCounter(0L, 0L) += label,
      mergeValue = (c: BinaryLabelCounter, label: Double) => c += label,
      mergeCombiners = (c1: BinaryLabelCounter, c2: BinaryLabelCounter) => c1 += c2
    ).sortByKey(ascending = true)

    val binnedDistinctScoresAndLabelCounts =
      if (numBins == 0) {
        distinctScoresAndLabelCounts.map { case (score, count) => ((score, score), count) }
      } else {
        val distinctScoresCount = distinctScoresAndLabelCounts.count()

        var groupCount =
          if (distinctScoresCount % numBins == 0) {
            distinctScoresCount / numBins
          } else {
            // prevent the last bin from being very small compared to the others
            distinctScoresCount / numBins + 1
          }

        if (groupCount < 2) {
          logInfo(s"Too few distinct scores ($distinctScoresCount) for $numBins bins to be useful;"
            + " proceed with number of bins == number of distinct scores.")
          distinctScoresAndLabelCounts.map { case (score, count) => ((score, score), count) }
        } else {
          if (groupCount >= Int.MaxValue) {
            val n = distinctScoresCount
            logWarning(
              s"Too many distinct scores ($n) for $numBins bins; capping at ${Int.MaxValue}")
            groupCount = Int.MaxValue
          }
          distinctScoresAndLabelCounts.mapPartitions(_.grouped(groupCount.toInt).map { pairs =>
            val firstScore = pairs.head._1
            val lastScore = pairs.last._1
            val agg = new BinaryLabelCounter()
            pairs.foreach { case (score, count) => agg += count }
            ((firstScore, lastScore), agg)
          })
        }
      }

    binnedDistinctScoresAndLabelCounts.map { case (bounds, counts) =>
      val n = counts.numPositives + counts.numNegatives
      (bounds, (counts.numPositives / n.toDouble, n))
    }
  }
}
