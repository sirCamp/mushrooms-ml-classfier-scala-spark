package com.sircamp.managers

import com.sircamp.Application

import scalax.chart.module.Charting
import scalax.chart.api._
/**
  * Created by stefano on 29/01/17.
  */
object ChartManager{


  /**
    * This method allows to print the passed data as a LineChart
    * @param pathFile
    * @param chartTotalData
    * @param title
    */
  def drawAndSave(pathFile:String,chartTotalData:List[(String,Vector[(Int,Double)])],title:String):Unit= {

    val chart = XYLineChart(chartTotalData)
    chart.title = title
    chart.saveAsPNG(pathFile)

  }

  /**
    *
    *
    * @param variant
    * @param chartTotalData
    */
  def drawAndSave(variant:String,chartTotalData:List[(String,Vector[(Int,Double)])]):Unit= {


    var chartTotalFinalData:List[(String,Vector[(Int,Double)])] = List()
    var pathFile:String = ""
    var title:String = ""

    if(variant.equals("default")){

      chartTotalFinalData = chartTotalData
      pathFile = Application.configuration.getString("resources.chart.output.default")+"/chart_"+System.currentTimeMillis()+".png"
      title = "Comparing All methods (Accuracy/TrainingPercentage)"

    }
    else if(variant.equals("decisiontree")){

      var chartDecisionTree:List[(String,Vector[(Int,Double)])] = List()

      chartTotalFinalData = chartTotalFinalData:+ chartTotalData.find(p=>p._1.equals("Variance")).get
      chartTotalFinalData = chartTotalFinalData:+ chartTotalData.find(p=>p._1.equals("Entropy")).get
      chartTotalFinalData = chartTotalFinalData:+ chartTotalData.find(p=>p._1.equals("Gini")).get

      pathFile = Application.configuration.getString("resources.chart.output.decisiontree")+"/chart_"+System.currentTimeMillis()+".png"
      title = "Comparing DecisionTree methods (Accuracy/TrainingPercentage)"

    }
    else if(variant.equals("neuralnetwork")){

      var chartNeuralNetwork:List[(String,Vector[(Int,Double)])] = List()

      chartTotalFinalData = chartTotalFinalData:+ chartTotalData.find(p=>p._1.equals("Multiperceptron")).get

      pathFile = Application.configuration.getString("resources.chart.output.neuralnetwork")+"/chart_"+System.currentTimeMillis()+".png"
      title = "NeuralNetwork (Accuracy/TrainingPercentage)"

    }
    else if(variant.equals("naivebayes")){

      var chartNaiveBayes:List[(String,Vector[(Int,Double)])] = List()

      chartTotalFinalData = chartTotalFinalData:+ chartTotalData.find(p=>p._1.equals("NaiveBayes")).get

      pathFile = Application.configuration.getString("resources.chart.output.naivebayes")+"/chart_"+System.currentTimeMillis()+".png"
      title = "NaiveBays (Accuracy/TrainingPercentage)"

    }
    else if(variant.equals("clustering")){

      var chartClustering:List[(String,Vector[(Int,Double)])] = List()

      chartTotalFinalData = chartTotalFinalData:+ chartTotalData.find(p=>p._1.equals("Kmeans")).get
      chartTotalFinalData = chartTotalFinalData:+ chartTotalData.find(p=>p._1.equals("KmeansBisect")).get

      pathFile = Application.configuration.getString("resources.chart.output.clustering")+"/chart_"+System.currentTimeMillis()+".png"
      title = "Comparing Clustering KMeans methods (Accuracy/TrainingPercentage)"

    }



    val chart = XYLineChart(chartTotalFinalData)
    chart.title = title
    chart.saveAsPNG(pathFile)

  }

}
