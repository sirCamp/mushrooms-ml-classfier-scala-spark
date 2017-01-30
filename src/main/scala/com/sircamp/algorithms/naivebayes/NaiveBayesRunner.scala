package com.sircamp.algorithms.naivebayes

import org.apache.log4j.Logger
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

/**
  * Created by stefano on 29/01/17.
  */
class NaiveBayesRunner(trainingData:RDD[LabeledPoint],testData:RDD[LabeledPoint]) {

  val logger:Logger = Logger.getLogger(classOf[NaiveBayesRunner])

  /**
    * This method allow The NaiveBayes model to run
    */
  def run():Double= {

    var chartTotalData:List[(String,Vector[(Int,Double)])] = List()

    var chartMultinomialData:Vector[(Int,Double)] = Vector()
    var chartBernoulliData:Vector[(Int,Double)] = Vector()

    var model = NaiveBayesBuilder.buildMultinomialModelClassifier(trainingData, 1.0)


    val labeledAndPreds = testData.map(
      point => {

        val prediction = model.predict(point.features)
        /**
          * TODO: Fix this logger
          *
          *  val log = Logger.getLogger(classOf[NaiveBayesRunner])
          *  log.info("Labeled: " + point.label + " Predicted: " + prediction + " Result: " + (point.label == prediction))
          *
          */
        (point.label, prediction)

      }

    )


    val metrics = new MulticlassMetrics(labeledAndPreds)
    chartMultinomialData = chartMultinomialData:+ (0,metrics.accuracy)
    logger.info("Nayve Accuracy: "+metrics.accuracy)

    metrics.accuracy
  }
}
