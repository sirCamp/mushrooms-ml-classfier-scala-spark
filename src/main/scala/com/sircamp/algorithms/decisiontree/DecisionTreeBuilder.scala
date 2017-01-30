package com.sircamp.algorithms.decisiontree

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.rdd.RDD

/**
  * Created by stefano on 27/01/17.
  */
object DecisionTreeBuilder {


  val numClasses = 2
  val categoricalFeaturesInfo:Map[Int,Int] = Map[Int, Int]()
  val impurityClassifier = "entropy"
  val impurityRegression = "variance"
  val maxBins = 15

  /**
    * This method build model for Gini or even Entropy
    * @param categoricalFeaturesInfo
    * @param trainingData
    * @param maxDepth
    * @return
    */
  def buildClassifierPredictionModel(categoricalFeaturesInfo:Map[Int, Int], trainingData:RDD[LabeledPoint], maxDepth:Int, impurityClassifier:String): DecisionTreeModel = {

    DecisionTree.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo, impurityClassifier, maxDepth, maxBins)

  }


  /**
    * This method build model for Variance Inpurity
    * @param categoricalFeaturesInfo
    * @param trainingData
    * @param maxDepth
    * @return
    */
  def buildRegressionPredictionModel(categoricalFeaturesInfo:Map[Int, Int], trainingData:RDD[LabeledPoint], maxDepth:Int): DecisionTreeModel = {

    DecisionTree.trainRegressor(trainingData, categoricalFeaturesInfo, impurityRegression, maxDepth, maxBins)

  }

}
