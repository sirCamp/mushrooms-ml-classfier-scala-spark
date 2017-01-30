package com.sircamp

import com.sircamp.algorithms.clustering.ClusteringKmeansRunner
import com.sircamp.algorithms.decisiontree.{DecisionTreeBuilder, DecisionTreeRunner}
import com.sircamp.algorithms.naivebayes.NaiveBayesRunner
import com.sircamp.algorithms.neuralnetwork.NeuralNetworkRunner
import com.sircamp.managers.{ChartManager, DataManager, LoadManager}
import com.sircamp.mappers.FeatureMapper
import com.sircamp.models.Mushroom
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import scalax.chart.module.Charting


object Application extends App with Charting {


  lazy val logger:Logger =  Logger.getLogger(Application.getClass)
  lazy val configuration:Config = ConfigFactory.load()

  /**
    *
    * @param args
    */
  override def main(args: Array[String]): Unit = {


    logger.info("***  Application Started ***")

    /**
      * Instance Spark
      */
    val conf = new SparkConf().setAppName("MachineLearningApplication").setMaster("local")
    val context = new SparkContext(conf)
    logger.info("*** SPARK Context Activated ***")


    /**
      * + load RAW dataset from csv
      * + Shuffle elements
      *
      */
    LoadManager.initialize(context,configuration.getString("resources.dataset"))
    LoadManager.shuffleElements(10)
    logger.info("*** Data retrived from CSV ***")

    /**
      * Get RDD[Mushroom]
      */
    var mushroomsList = LoadManager.loadData().map(Mushroom.parseMushroom).cache()

    /**
      *  Initialize and Extract Features
      */
    FeatureMapper.initialize(mushroomsList)
    val preparedModel = FeatureMapper.featureExtract(mushroomsList)
    val categoricalFeatures = FeatureMapper.categoricalFeaturesInfo()
    logger.info("*** Data loaded and Feature extracted ***")

    /**
      * Pre - Split this is necessary because ANN have a different dataset encoding
      */

    var step = 1

    /**
      * Declaring container for data set accuracy
      */
    var chartVarianceData:Vector[(Int,Double)] = Vector()
    var chartEntropyData:Vector[(Int,Double)] = Vector()
    var chartGiniData:Vector[(Int,Double)] = Vector()
    var chartNeuralNetworkData:Vector[(Int,Double)] = Vector()
    var chartNaiveBayesData:Vector[(Int,Double)] = Vector()
    var chartClusteringKmeansData:Vector[(Int,Double)] = Vector()
    var chartClusteringKmeansBisectData:Vector[(Int,Double)] = Vector()

    for(elem <- 0.10 to 0.90 by 0.10) {

      var splittedPreparedModel = preparedModel.randomSplit(Array(elem, 1 - elem), seed = 1234L)

      val rawTraining = splittedPreparedModel(0).cache()
      val rawTest = splittedPreparedModel(1).cache()
      logger.info("*** "+step+" STEP=[Training of: " + rawTraining.count() + "("+(elem*100)+"%) elements and Test of: " + rawTest.count() + "("+((1-elem)*100)+"%)] created ***")

      val rddTrainig = DataManager.loadRDDDataModel(rawTraining)
      val rddTest = DataManager.loadRDDDataModel(rawTest)
      logger.info("*** RDD training and test loaded ***")

      val libsvmTraining = DataManager.loadLibSVMDataModel(rawTraining)
      val libsvmTest = DataManager.loadLibSVMDataModel(rawTest)
      logger.info("*** DataFrame training and test loaded ***")

      val vectorTraining = DataManager.loadVectorDataModel(rawTraining)
      val vectorTest = DataManager.loadVectorDataModel(rawTraining)
      logger.info("*** Vector training and test loaded ***")


      val treeThread:Thread = new Thread() {

        override def run(): Unit = {

        logger.info("*** DecisionTree RUN ***")
        val decisionTreeRunner = new DecisionTreeRunner(rddTrainig, rddTest, categoricalFeatures, 5)
        val dataVEG = decisionTreeRunner.run()

        chartVarianceData = chartVarianceData:+(step*10,dataVEG.head)
        chartEntropyData = chartEntropyData:+(step*10,dataVEG(1))
        chartGiniData = chartGiniData:+(step*10,dataVEG(2))


        }
      }

      val neuralNetworkThread:Thread = new Thread() {

        override def run(): Unit = {

          logger.info("*** NeuralNetwork (MultilayerPerceptron) RUN ***")
          val neuralNetworkRunner = new NeuralNetworkRunner(libsvmTraining, libsvmTest, categoricalFeatures, 20)
          val dataNn = neuralNetworkRunner.run()

          chartNeuralNetworkData = chartNeuralNetworkData:+(step*10,dataNn)

        }
      }


      val naiveBayesThread:Thread = new Thread() {

        override def run(): Unit = {

          logger.info("*** NaiveBayes Multiclass RUN ***")
          val naiveBayesRunner = new NaiveBayesRunner(rddTrainig, rddTest)
          val dataNb = naiveBayesRunner.run()
          chartNaiveBayesData = chartNaiveBayesData:+(step*10,dataNb)
        }
      }


      val clusteringThread:Thread = new Thread() {

        override def run(): Unit = {

          logger.info("*** Clustering (Kmeans + BisectKmeans) RUN ***")
          val clusteringKmeansRunner = new ClusteringKmeansRunner(vectorTraining, vectorTraining, 2, 20)
          val dataC = clusteringKmeansRunner.run()
          chartClusteringKmeansData = chartClusteringKmeansData:+(step*10,dataC.head)
          chartClusteringKmeansBisectData = chartClusteringKmeansBisectData:+(step*10,dataC(1))
        }
      }

      /*
      * TODO: find a best way to run multiple learning at same step
      *
      * */
      treeThread.start()
      neuralNetworkThread.start()
      naiveBayesThread.start()
      clusteringThread.start()


      /**
        * Need to be joined on main thread
        */
      treeThread.join()
      naiveBayesThread.join()
      clusteringThread.join()
      neuralNetworkThread.join()


      logger.info("*** Cleaning tmp data ***")
      LoadManager.clearData()

      step += 1
    }


    var chartTotalData:List[(String,Vector[(Int,Double)])] = List()
    var chartDecisionTree:List[(String,Vector[(Int,Double)])] = List()
    var chartNeuralNetwork:List[(String,Vector[(Int,Double)])] = List()
    var chartNaiveBayes:List[(String,Vector[(Int,Double)])] = List()
    var chartClustering:List[(String,Vector[(Int,Double)])] = List()

    /**
      * Preparing all data for chart
      */
    chartTotalData = chartTotalData:+ ("Variance",chartVarianceData)
    chartTotalData = chartTotalData:+ ("Entropy",chartEntropyData)
    chartTotalData = chartTotalData:+ ("Gini",chartGiniData)
    chartTotalData = chartTotalData:+ ("Multiperceptron",chartNeuralNetworkData)
    chartTotalData = chartTotalData:+ ("NaiveBayes",chartNaiveBayesData)
    chartTotalData = chartTotalData:+ ("Kmeans",chartClusteringKmeansData)
    chartTotalData = chartTotalData:+ ("KmeansBisect",chartClusteringKmeansBisectData)

    /**
      * Preparing decision tree data for chart alone
      */
    chartDecisionTree = chartDecisionTree:+ ("Variance",chartVarianceData)
    chartDecisionTree = chartDecisionTree:+ ("Entropy",chartEntropyData)
    chartDecisionTree = chartDecisionTree:+ ("Gini",chartGiniData)

    /**
      * Preparing neuralnetwork tree data for chart alone
      */
    chartNeuralNetwork = chartNeuralNetwork:+ ("Multiperceptron",chartNeuralNetworkData)

    /**
      * Preparing naivebayes tree data for chart alone
      */
    chartNaiveBayes = chartNaiveBayes :+ ("NaiveBayes",chartNaiveBayesData)

    /**
      * Preparing clustering tree data for chart alone
      */
    chartClustering = chartClustering:+ ("Kmeans",chartClusteringKmeansData)
    chartClustering = chartClustering:+ ("KmeansBisect",chartClusteringKmeansBisectData)

    ChartManager.drawAndSave(configuration.getString("resources.chart.output.default")+"/chart_"+System.currentTimeMillis()+".png",chartTotalData,"Comparing All methods (Accuracy/TrainingPercentage)")
    ChartManager.drawAndSave(configuration.getString("resources.chart.output.decisiontree")+"/chart_"+System.currentTimeMillis()+".png",chartDecisionTree,"Comparing DecisionTree methods (Accuracy/TrainingPercentage)")
    ChartManager.drawAndSave(configuration.getString("resources.chart.output.neuralnetwork")+"/chart_"+System.currentTimeMillis()+".png",chartNeuralNetwork,"NeuralNetwork (Accuracy/TrainingPercentage)")
    ChartManager.drawAndSave(configuration.getString("resources.chart.output.naivebayes")+"/chart_"+System.currentTimeMillis()+".png",chartNaiveBayes,"NaiveBays (Accuracy/TrainingPercentage)")
    ChartManager.drawAndSave(configuration.getString("resources.chart.output.clustering")+"/chart_"+System.currentTimeMillis()+".png",chartClustering,"Comparing Clustering KMeans methods (Accuracy/TrainingPercentage)")


    logger.info("*** SPARK stopping Context ***")
    context.stop()
    logger.info("*** Stopped ***")

    logger.info("*** Application terminated ***")

  }
}