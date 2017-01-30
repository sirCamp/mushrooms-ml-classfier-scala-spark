package com.sircamp.managers

import scalax.chart.module.Charting
import scalax.chart.api._
/**
  * Created by stefano on 29/01/17.
  */
object ChartManager{


  /**
    * This method allows to print the passed data as a LineChart
    * @param pathFile
    * @param data
    * @param title
    */
  def drawAndSave(pathFile:String,data:List[(String,Vector[(Int,Double)])],title:String):Unit= {

    val chart = XYLineChart(data)
    chart.title = title
    chart.saveAsPNG(pathFile)

  }

}
