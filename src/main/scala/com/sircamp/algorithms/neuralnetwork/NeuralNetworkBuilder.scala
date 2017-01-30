package com.sircamp.algorithms.neuralnetwork

import java.io.{BufferedWriter, FileOutputStream, OutputStreamWriter}

import com.sircamp.Application
import org.apache.spark.ml.classification.{MultilayerPerceptronClassificationModel, MultilayerPerceptronClassifier}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.linalg.VectorUDT
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.types._

/**
  * Created by stefano on 29/01/17.
  */
object NeuralNetworkBuilder {

  private val blockSize = 128
  private val seed = 1234L

  final var TEMP_FILE_PATH = Application.configuration.getString("tmp.libsvm")


  /**
    * This method convert a LabeledPoint dataset to LIB-SVM
    * @deprecated
    * @param trainingData
    */
  def convertRddToLibsvm(trainingData:RDD[LabeledPoint]):Unit= {


    /**
      * remove preshufelled files
      */
    val file = new java.io.File(TEMP_FILE_PATH)
    if( file.exists){
      file.delete()
    }

    val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)))


    var sb = new StringBuilder()
    trainingData.collect().foreach(f=>{
      var arr = f.features.toArray
      var features = ""
      for(i <- arr.indices){
        features = features +((i+1)+":"+arr(i))+" "
      }
      writer.write((f.label+" "+features) + "\n")
    })
    writer.close()
  }

  def buildMultiPerpectronNetwork(trainingData:Dataset[Row], layers:Array[Int], maxIter:Int):MultilayerPerceptronClassificationModel = {

    val trainer = new MultilayerPerceptronClassifier()
      .setLayers(layers)
      .setBlockSize(blockSize)
      .setSeed(seed)
      .setMaxIter(maxIter)

    trainer.fit(trainingData)

  }
}
