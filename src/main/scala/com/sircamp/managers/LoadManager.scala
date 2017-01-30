package com.sircamp.managers

import com.sircamp.models.Mushroom
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.io.Source._
import util.Random._
import java.io._

import com.sircamp.Application


import org.apache.log4j.Logger

/**
  * Created by stefano on 28/01/17.
  */
object LoadManager {

  var context:SparkContext = null
  val logger:Logger = Logger.getLogger(LoadManager.getClass)

  var DATA_PATH:String = null
  var NOISED_FILE_PATH:String = null
  final var TEMP_FILE_PATH = Application.configuration.getString("tmp.libsvm")

  /**
    * this method initialize the LoadData manager
    * @param context
    * @param dataPath
    */
  def initialize(context: SparkContext, dataPath: String): Unit ={

    LoadManager.context = context
    LoadManager.DATA_PATH = dataPath

  }

  /**
    * This method operate a shuffle the starting file
    * @param times
    */
  def shuffleElements(times:Int): Unit = {

    var lines = fromFile(DATA_PATH).getLines

    if(times != 0){
      for (i <- 1 to times){
        lines = shuffle(lines)
      }
    }
    else{
      lines = shuffle(lines)
    }


    var filePath = DATA_PATH.replace(".csv","_shuffled.csv")


    /**
      * remove preshufelled files
      */
    val file = new java.io.File(filePath)
    if( file.exists){
        file.delete()
    }

    /**
      * write shuffeled file
      */
    val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)))
    for (x <- lines) {
      writer.write(x + "\n")
    }
    writer.close()

    NOISED_FILE_PATH = filePath
  }

  /**
    * This method should introduces 5% of noise inside the data set
    * @param mushroomList
    * @return
    */
  def addFivePercentOfNoise(mushroomList: RDD[Mushroom]): RDD[Mushroom] = {

    throw new NotImplementedError()
  }

  /**
    * this method should introduces 10% of noise in the data set
    * @param mushroomList
    * @return
    */
  def addTenPercentOfNoise(mushroomList: RDD[Mushroom]): RDD[Mushroom] = {

    throw new NotImplementedError()
  }

  /**
    * this method should introduces 15% of noise in the data set
    * @param mushroomList
    * @return
    */
  def addFifteenPercentOfNoise(mushroomList: RDD[Mushroom]): RDD[Mushroom] = {
    throw new NotImplementedError()
  }

  /**
    * This method load the file of data set ( even if it was shuffled )
    * @return
    */
  def loadData():RDD[String] = {

    var fileToLoad:String = ""

    if (NOISED_FILE_PATH.equals(null)) fileToLoad = DATA_PATH else fileToLoad = NOISED_FILE_PATH

    context.textFile(fileToLoad)
  }


  /**
    * This method clean the temp files
    * @return
    */
  def clearData():Unit = {

    var file = new File(NOISED_FILE_PATH)
    if(file.exists())
      file.delete()

    file = new File(TEMP_FILE_PATH)
    if(file.exists())
      file.delete()
  }
}
