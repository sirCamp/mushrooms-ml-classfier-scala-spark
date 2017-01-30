package com.sircamp.algorithms.naivebayes

import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD

/**
  * Created by stefano on 29/01/17.
  */
object NaiveBayesBuilder {

  private final var MULTINOMIAL_MODEL_TYPE:String = "multinomial"

  private final var BERNOULLI_MODEL_TYPE:String = "bernoulli"

  /**
    *
    * @param trainingData
    * @param lambda
    * @return
    */
  def buildMultinomialModelClassifier(trainingData:RDD[LabeledPoint], lambda:Double):NaiveBayesModel = {

    NaiveBayes.train(trainingData, lambda = 1.0, MULTINOMIAL_MODEL_TYPE)
  }


  /**
    *
    * @param trainingData
    * @param lambda
    * @return
    */
  def buildBernulliModelClassifier(trainingData:RDD[LabeledPoint], lambda:Double):NaiveBayesModel = {

    NaiveBayes.train(trainingData, lambda = 1.0, BERNOULLI_MODEL_TYPE)

  }
}
