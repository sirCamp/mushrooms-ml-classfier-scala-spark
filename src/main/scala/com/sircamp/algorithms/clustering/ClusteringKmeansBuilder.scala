package com.sircamp.algorithms.clustering


import org.apache.spark.mllib.clustering.{BisectingKMeans, BisectingKMeansModel, KMeans, KMeansModel}

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vector

/**
  * Created by stefano on 29/01/17.
  */
object ClusteringKmeansBuilder {


  def buildKmeansPredictionModel(trainingData:RDD[Vector],K:Int,numIterations:Int): KMeansModel ={


    KMeans.train(trainingData, K, numIterations)

  }

  def buildBisectionKmeansPredictionModel(trainingData:RDD[Vector],K:Int,numIterations:Int): BisectingKMeansModel ={

    val bkm = new BisectingKMeans().setK(K)
    bkm.run(trainingData)

  }
}
