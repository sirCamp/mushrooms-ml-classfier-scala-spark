package com.sircamp.algorithms.neuralnetwork

import com.sircamp.mappers.FeatureMapper
import com.sircamp.models.Mushroom
import org.apache.log4j.Logger
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by stefano on 29/01/17.
  */
class NeuralNetworkRunner(trainingData:DataFrame, testData:DataFrame, categoricalFeatures:Map[Int,Int], gradientIteration:Int) {

  val logger:Logger = Logger.getLogger(classOf[NeuralNetworkRunner])

  /**
    * This method allow The ANN model to run
    */
  def run():Double = {


    val model = NeuralNetworkBuilder.buildMultiPerpectronNetwork(trainingData,Array[Int](22, 5, 4, 3),gradientIteration)
    val result = model.transform(testData)

    //result.collect()
    val predictionAndLabels = result.select("prediction", "label")
    val evaluator = new MulticlassClassificationEvaluator()
      .setMetricName("accuracy")

    val accuracy = evaluator.evaluate(predictionAndLabels)

    logger.info("Test set accuracy = " + accuracy)
    accuracy
  }
}
