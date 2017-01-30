package com.sircamp.managers

import java.io.{BufferedWriter, FileOutputStream, OutputStreamWriter}

import com.sircamp.algorithms.neuralnetwork.NeuralNetworkBuilder
import org.apache.log4j.Logger
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by stefano on 30/01/17.
  */
object DataManager {

  final var TEMP_FILE_PATH = "/home/stefano/scala-decisiontree/src/main/resources/tmp/libsvm_temp.txt"

  val logger:Logger = Logger.getLogger(DataManager.getClass)

  /**
    * This method load file in RDD paradigm
    * @param dataToLoad
    * @return
    */
  def loadRDDDataModel(dataToLoad:RDD[Array[Double]]):RDD[LabeledPoint] = {

    dataToLoad.map(

      x => LabeledPoint(x(0),Vectors.dense(x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8),x(9),x(10),x(11),x(12),x(13),x(14),x(15),x(16),x(17),x(18),x(19),x(20),x(21),x(22)))

    )



  }

  /**
    * This method load file in Vector paradigm
    * @param dataToLoad
    * @return
    */
  def loadVectorDataModel(dataToLoad:RDD[Array[Double]]):RDD[Vector] = {

    dataToLoad.map(

      x => Vectors.dense(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8),x(9),x(10),x(11),x(12),x(13),x(14),x(15),x(16),x(17),x(18),x(19),x(20),x(21),x(22))

    )

  }

  /**
    * This method load file in LIB-SVM paradigm
    * @note this method need to be update to become for efficient
    * @param dataToLoad
    * @return
    */
  def loadLibSVMDataModel(dataToLoad:RDD[Array[Double]]):DataFrame = {

    val data =  dataToLoad.map(

      x => LabeledPoint(x(0),Vectors.dense(x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8),x(9),x(10),x(11),x(12),x(13),x(14),x(15),x(16),x(17),x(18),x(19),x(20),x(21),x(22)))

    )
    /**
      * Retrive SPARK instance
      */
    val spark = SparkSession
      .builder
      .appName("MachineLearningApplication")
      .getOrCreate()

    /**
      * remove pre-temporary file
      */
    val file = new java.io.File(TEMP_FILE_PATH)
    if(file.exists){
      file.delete()
    }


    val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)))

    var sb = new StringBuilder()
    data.collect().foreach(f=>{
      var arr = f.features.toArray
      var features = ""
      for(i <- arr.indices){
        features = features +((i+1)+":"+arr(i))+" "
      }
      writer.write((f.label+" "+features) + "\n")
    })

    writer.close()

    spark.read.format("libsvm").load(DataManager.TEMP_FILE_PATH)

  }

}
