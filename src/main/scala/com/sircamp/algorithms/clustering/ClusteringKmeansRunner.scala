package com.sircamp.algorithms.clustering

import org.apache.log4j.Logger
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD


/**
  * Created by stefano on 29/01/17.
  */
class ClusteringKmeansRunner(trainingData:RDD[Vector],testData:RDD[Vector],K:Int,iterations:Int){

  val logger:Logger = Logger.getLogger(classOf[ClusteringKmeansRunner])

  /**
    * This method allow the ClusteringModels to run
    */
  def run():Seq[Double] = {


    val model = ClusteringKmeansBuilder.buildKmeansPredictionModel(trainingData,K,iterations)
    val bisectingModel = ClusteringKmeansBuilder.buildBisectionKmeansPredictionModel(trainingData,K,iterations)

    val testSet = testData.collect()
    var labeledPoint:List[(Int,Vector)] = List()
    var labeledPointBisec:List[(Int,Vector)] = List()

    for(i<-testSet.indices){

      val predicted = model.predict(testSet(i))
      val bisectPred = bisectingModel.predict(testSet(i))

      labeledPoint = labeledPoint :+ (predicted,testSet(i))
      labeledPointBisec = labeledPoint :+ (bisectPred,testSet(i))

    }

    val kmeans = calculateRandIndex(labeledPoint)
    val kmeansBisection = calculateRandIndex(labeledPointBisec)

    logger.info("Kmeans: "+kmeans)
    logger.info("KmeansBis: "+kmeansBisection)

    /**
      * decomment if you want also this one
      */
    //calculateWSSSE(model,training)

    Seq(kmeans,kmeansBisection)
  }

  /**
    * This method needs to calculate the RadixIndex for Clustering (~accuracy)
    * @param labeledPoint
    * @return
    */
  def calculateRandIndex(labeledPoint: List[(Int,Vector)]):Double = {

    var rightClassified = 0

    for(i<-labeledPoint.indices){

      logger.debug("Predicted: "+labeledPoint(i)._1+" Test: "+labeledPoint(i)._2(0).toInt+" Result: "+(labeledPoint(i)._1==labeledPoint(i)._2(0).toInt))
      if(labeledPoint(i)._1.toInt == labeledPoint(i)._2(0).toInt){
        rightClassified += 1
      }

    }

    rightClassified.toDouble/labeledPoint.size.toDouble

  }

  /**
    * This method Calculate the cost and the errors of Clusters
    * @param model
    * @param training
    * @return
    */
  def calculateWSSSE(model:KMeansModel,training:RDD[Vector]):Double= {
    model.computeCost(training)
  }
}
