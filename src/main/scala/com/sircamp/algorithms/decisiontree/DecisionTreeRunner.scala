package com.sircamp.algorithms.decisiontree

import com.sircamp.managers.{ChartManager}
import com.sircamp.mappers.FeatureMapper
import com.sircamp.models.Mushroom
import org.apache.log4j.Logger
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD


/**
  * Created by stefano on 28/01/17.
  */
class DecisionTreeRunner(trainingData:RDD[LabeledPoint],testData:RDD[LabeledPoint],categoricalFeatures:Map[Int,Int],maxDepth:Int) {

  val logger:Logger = Logger.getLogger(classOf[DecisionTreeRunner])

  /**
  * This method allow The DecisionTree model to run
  */
  def run():Seq[Double] = {

    var chartVarianceData:Double = 0
    var chartEntropyData:Double = 0
    var chartGiniData:Double = 0

    logger.info("*** Start Variance ***")
    /**
      * Get DecisionTree model instance
      */
    var modelV = DecisionTreeBuilder.buildRegressionPredictionModel(categoricalFeatures,trainingData, maxDepth)

    val labeledAndPredsV = testData.map(
      point =>{

        val prediction = modelV.predict(point.features)
        (point.label,prediction)
      }

    )
    var metricsV = new MulticlassMetrics(labeledAndPredsV)
    chartVarianceData = metricsV.accuracy


    logger.info("*** Start Entropy ***")
    /**
      * Get DecisionTree model instance
      */
    var modelE = DecisionTreeBuilder.buildClassifierPredictionModel(categoricalFeatures,trainingData, maxDepth, "entropy")

    val labeledAndPredsE = testData.map(
      point =>{

        val prediction = modelE.predict(point.features)
        (point.label,prediction)
      }

    )
    val metricsE = new MulticlassMetrics(labeledAndPredsE)
    chartEntropyData = metricsE.accuracy

    logger.info("*** Start Gini ***")
    /**
      * Get DecisionTree model instance
      */
    var modelG = DecisionTreeBuilder.buildClassifierPredictionModel(categoricalFeatures,trainingData, maxDepth, "gini")

    val labeledAndPredsG = testData.map(
      point =>{

        val prediction = modelG.predict(point.features)
        (point.label,prediction)
      }

    )
    val metricsG = new MulticlassMetrics(labeledAndPredsG)
    chartGiniData = metricsG.accuracy

    Seq(chartVarianceData,chartEntropyData,chartGiniData)
  }


  def runAsService() = {
    var chartTotalData:List[(String,Vector[(Int,Double)])] = List()


    var chartVarianceData:Vector[(Int,Double)] = Vector()
    var chartEntropyData:Vector[(Int,Double)] = Vector()
    var chartGiniData:Vector[(Int,Double)] = Vector()


    val variance:Thread = new Thread(){

      override def run(): Unit ={

        logger.info("*** Start Variance ***")
        for ( i <- 1 to 9){

          /**
            * Get DecisionTree model instance
            */
          val model = DecisionTreeBuilder.buildRegressionPredictionModel(categoricalFeatures,trainingData, 4)

          val labeledAndPreds = testData.map(
            point =>{

              val prediction = model.predict(point.features)
              (point.label,prediction)
            }

          )
          val metrics = new MulticlassMetrics(labeledAndPreds)
          chartVarianceData = chartVarianceData:+ (i,metrics.accuracy)

        }

        logger.info("*** Stop Variance ***")
      }

    }

    val entropy:Thread = new Thread(){

      override def run(): Unit ={

        logger.info("*** Start Entropy ***")
        for ( i <- 1 to 9){

          /**
            * Get DecisionTree model instance
            */
          val model = DecisionTreeBuilder.buildClassifierPredictionModel(categoricalFeatures,trainingData, 5, "entropy")

          val labeledAndPreds = testData.map(
            point =>{

              val prediction = model.predict(point.features)
              (point.label,prediction)
            }

          )
          val metrics = new MulticlassMetrics(labeledAndPreds)
          chartEntropyData = chartEntropyData:+ (i,metrics.accuracy)

        }
        logger.info("*** Stop Entropy ***")
      }

    }

    val gini:Thread = new Thread(){

      override def run(): Unit ={

        logger.info("*** Start Gini ***")
        for ( i <- 1 to 9){

          /**
            * Get DecisionTree model instance
            */
          val model = DecisionTreeBuilder.buildClassifierPredictionModel(categoricalFeatures,trainingData, 5, "gini")

          val labeledAndPreds = testData.map(
            point =>{

              val prediction = model.predict(point.features)
              (point.label,prediction)
            }

          )
          val metrics = new MulticlassMetrics(labeledAndPreds)
          chartGiniData = chartGiniData:+ (i,metrics.accuracy)

        }

        logger.info("*** Stop Gini ***")
      }

    }


    logger.info("*** Classification Process Started ***")
    variance.start()
    entropy.start()
    gini.start()

    variance.join()
    entropy.join()
    gini.join()
    logger.info("*** Classification Process Finished ***")

    logger.info("*** Preparing Chart Data ***")
    chartTotalData = chartTotalData:+ ("Variance",chartVarianceData)
    chartTotalData = chartTotalData:+ ("Entropy",chartEntropyData)
    chartTotalData = chartTotalData:+ ("Gini",chartGiniData)
    logger.info("*** Data prepared! Starting drowing chart ***")

    ChartManager.drawAndSave("/home/stefano/scala-decisiontree/src/main/resources/output/decisiontree/chart_"+System.currentTimeMillis()+".png",chartTotalData,"Different DecisionTree Approaches")
    logger.info("*** Chart drowed ***")

    Seq(chartVarianceData,chartEntropyData,chartGiniData)
  }

}
