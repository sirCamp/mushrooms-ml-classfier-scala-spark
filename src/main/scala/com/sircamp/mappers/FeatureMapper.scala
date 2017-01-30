package com.sircamp.mappers

import com.sircamp.models.Mushroom
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD


/**
  * Created by stefano on 27/01/17.
  */
object FeatureMapper {

  val logger = Logger.getLogger(FeatureMapper.getClass)

  var edibleMap: Map[String, Int] = Map()
  var capShapeMap: Map[String, Int] = Map()
  var capSurfaceMap: Map[String, Int] = Map()
  var capColorMap: Map[String, Int] = Map()
  var bruisesMap: Map[String, Int] = Map()
  var odorMap: Map[String, Int] = Map()
  var gillAttachmentMap: Map[String, Int] = Map()
  var gillSpacingMap: Map[String, Int] = Map()
  var gillSizeMap: Map[String, Int] = Map()
  var gillColorMap: Map[String, Int] = Map()
  var stalkShapeMap: Map[String, Int] = Map()
  var stalkRootMap: Map[String, Int] = Map()
  var stalkSurfaceAboveRingMap: Map[String, Int] = Map()
  var stalkSurfaceBelowRingMap: Map[String, Int] = Map()
  var stalkColorAboveRingMap: Map[String, Int] = Map()
  var stalkColorBelowRingMap: Map[String, Int] = Map()
  var veilTypeMap: Map[String, Int] = Map()
  var veilColorMap: Map[String, Int] = Map()
  var ringNumberMap: Map[String, Int] = Map()
  var ringTypeMap: Map[String, Int] = Map()
  var sporePrintColorMap: Map[String, Int] = Map()
  var populationMap: Map[String, Int] = Map()
  var habitatMap: Map[String, Int] = Map()

  /**
    * This method initialize the features maps
    * @param mushroomsRDD
    */
  def initialize(mushroomsRDD: RDD[Mushroom]): Unit = {

    /**
      * Mappa velonosità
      */
    mushroomsRDD.map(mushroom => mushroom.edible).distinct.collect.foreach(

      x => {

        edibleMap += (x -> ( if(x.equals("e")) 1 else 0))

      }

    )
    logger.debug("*** edibleMap: "+edibleMap.toString()+" ***")
    /**
      * Mappa formaCappello
      */
    var index: Int = 0
    mushroomsRDD.map(mushroom => mushroom.capShape).distinct.collect.foreach(

      x => {

        capShapeMap += (x -> index)
        index += 1

      }

    )

    logger.debug("*** capShapeMap: "+capShapeMap.toString()+" ***")


    /**
      * Mappa velonosità
      */
    var index1: Int = 0
    mushroomsRDD.map(mushroom => mushroom.capSurface).distinct.collect.foreach(

      x => {

        capSurfaceMap += (x -> index1);
        index1 += 1

      }

    )
    logger.debug("*** capSurfaceMap: "+capSurfaceMap.toString()+" ***")

    var index2:Int = 0
    mushroomsRDD.map(mushroom => mushroom.capColor).distinct.collect.foreach(

      x => {capColorMap+= ( x -> index2 )
        index2 += 1

      }

    )
    logger.debug("*** capColorMap: "+capColorMap.toString()+" ***")

    var index3:Int = 0
    mushroomsRDD.map(mushroom => mushroom.bruises).distinct.collect.foreach(

      x => {bruisesMap+= ( x -> index3 )
        index3 += 1

      }

    )
    logger.debug("*** bruisesMap: "+bruisesMap.toString()+" ***")

   var index4:Int = 0
    mushroomsRDD.map(mushroom => mushroom.odor).distinct.collect.foreach(

      x => {odorMap+= ( x -> index4 )
        index4 += 1

      }

    )
    logger.debug("*** odorMap: "+odorMap.toString()+" ***")

    var index5:Int = 0
    mushroomsRDD.map(mushroom => mushroom.gillAttachment).distinct.collect.foreach(

      x => {gillAttachmentMap+= ( x -> index5 )
        index5 += 1

      }

    )
    logger.debug("*** gillAttachmentMap: "+gillAttachmentMap.toString()+" ***")

    var index6:Int = 0
    mushroomsRDD.map(mushroom => mushroom.gillSpacing).distinct.collect.foreach(

      x => {gillSpacingMap+= ( x -> index6 )
        index6 += 1

      }

    )
    logger.debug("*** gillSpacingMap: "+gillSpacingMap.toString()+" ***")

    var index7:Int = 0
    mushroomsRDD.map(mushroom => mushroom.gillSize).distinct.collect.foreach(

      x => {gillSizeMap+= ( x -> index7 )
        index7 += 1

      }

    )
    logger.debug("*** gillSizeMap: "+gillSizeMap.toString()+" ***")

    var index8:Int = 0
    mushroomsRDD.map(mushroom => mushroom.gillColor).distinct.collect.foreach(

      x => {gillColorMap+= ( x -> index8 )
        index8 += 1

      }

    )
    logger.debug("*** gillColorMap: "+gillColorMap.toString()+" ***")

    var index9:Int = 0
    mushroomsRDD.map(mushroom => mushroom.stalkShape).distinct.collect.foreach(

      x => {stalkShapeMap+= ( x -> index9 )
        index9 += 1

      }

    )
    logger.debug("*** stalkShapeMap: "+stalkShapeMap.toString()+" ***")

    var index10:Int = 0
    mushroomsRDD.map(mushroom => mushroom.stalkRoot).distinct.collect.foreach(

      x => {stalkRootMap+= ( x -> index10 )
        index10 += 1

      }

    )
    logger.debug("*** stalkRootMap: "+stalkRootMap.toString()+" ***")

    var index11:Int = 0
    mushroomsRDD.map(mushroom => mushroom.stalkSurfaceAboveRing).distinct.collect.foreach(

      x => {stalkSurfaceAboveRingMap+= ( x -> index11 )
        index11 += 1

      }

    )
    logger.debug("*** stalkSurfaceAboveRingMap: "+stalkSurfaceAboveRingMap.toString()+" ***")

    var index12:Int = 0
    mushroomsRDD.map(mushroom => mushroom.stalkSurfaceBelowRing).distinct.collect.foreach(

      x => {stalkSurfaceBelowRingMap+= ( x -> index12 )
        index12 += 1

      }

    )
    logger.debug("*** stalkColorAboveRingMap: "+stalkColorAboveRingMap.toString()+" ***")

    var index13:Int = 0
    mushroomsRDD.map(mushroom => mushroom.stalkColorAboveRing).distinct.collect.foreach(

      x => {stalkColorAboveRingMap+= ( x -> index13 )
        index13 += 1

      }

    )
    logger.debug("*** stalkColorAboveRingMap: "+stalkColorAboveRingMap.toString()+" ***")

    var index14:Int = 0
    mushroomsRDD.map(mushroom => mushroom.stalkColorBelowRing).distinct.collect.foreach(

      x => {stalkColorBelowRingMap+= ( x -> index14 )
        index14 += 1

      }

    )
    logger.debug("*** stalkColorBelowRingMap: "+stalkColorBelowRingMap.toString()+" ***")

    var index15:Int = 0
    mushroomsRDD.map(mushroom => mushroom.veilType).distinct.collect.foreach(

      x => {veilTypeMap+= ( x -> index15 )
        index15 += 1

      }

    )
    logger.debug("*** veilTypeMap: "+veilTypeMap.toString()+" ***")

    var index16:Int = 0
    mushroomsRDD.map(mushroom => mushroom.veilColor).distinct.collect.foreach(

      x => {veilColorMap+= ( x -> index16 )
        index16 += 1

      }

    )
    logger.debug("*** veilColorMap: "+veilColorMap.toString()+" ***")

    var index17:Int = 0
    mushroomsRDD.map(mushroom => mushroom.ringNumber).distinct.collect.foreach(

      x => {ringNumberMap+= ( x -> index17 )
        index17 += 1

      }

    )
    logger.debug("*** ringNumberMap: "+ringNumberMap.toString()+" ***")

    var index18:Int = 0
    mushroomsRDD.map(mushroom => mushroom.ringType).distinct.collect.foreach(

      x => {ringTypeMap+= ( x -> index18 )
        index18 += 1

      }

    )
    logger.debug("*** ringTypeMap: "+ringTypeMap.toString()+" ***")

    var index19:Int = 0
    mushroomsRDD.map(mushroom => mushroom.sporePrintColor).distinct.collect.foreach(

      x => {sporePrintColorMap+= ( x -> index19 )
        index19 += 1

      }

    )
    logger.debug("*** sporePrintColorMap: "+sporePrintColorMap.toString()+" ***")

    var index20:Int = 0
    mushroomsRDD.map(mushroom => mushroom.population).distinct.collect.foreach(

      x => {populationMap+= ( x -> index20 )
        index20 += 1

      }

    )
    logger.debug("*** populationMap: "+populationMap.toString()+" ***")

    var index21:Int = 0
    mushroomsRDD.map(mushroom => mushroom.habitat).distinct.collect.foreach(

      x => {habitatMap+= ( x -> index21 )
        index21 += 1

      }

    )
    logger.debug("*** habitatMap: "+habitatMap.toString()+" ***")


  }


  /**
    * This method should operate the OneHot transformation
    * @param mushroomRDD
    */
  def featureOneHot(mushroomRDD: RDD[Mushroom]):Unit = {


    throw new NotImplementedError("This method is not implementsed")
    
    /*
      To FIX:
      val spark = SparkSession
      .builder
      .appName("OneHotEncoderExample")
      .getOrCreate()

    // $example on$
    val df = spark.createDataFrame(Seq(
      (0, "a","3"),
      (1, "b","5"),
      (2, "c","6"),
      (3, "a","3"),
      (4, "a","y"),
      (5, "c","6")
    )).toDF("id", "category","other")

    val indexer = new StringIndexer()
      .setInputCol("category")
      .setOutputCol("categoryIndex")
      .fit(df)
    val indexed = indexer.transform(df)

    val encoder = new OneHotEncoder()
      .setInputCol("categoryIndex")
      .setOutputCol("categoryVec")

    val encoded = encoder.transform(indexed)
    encoded.show()*/

  }

  /**
    * This method prepare the features of model
    * @param mushroomsRDD
    * @return
    */
  def featureExtract(mushroomsRDD: RDD[Mushroom]): RDD[Array[Double]] = {


    val preparedModel = mushroomsRDD.map(

      mushroom =>{

        val edible = edibleMap(mushroom.edible)
        val capSurface =capSurfaceMap(mushroom.capSurface)
        val capShape = capShapeMap(mushroom.capShape)
        val capColor = capColorMap(mushroom.capColor)
        val bruises = bruisesMap(mushroom.bruises)
        val odor = odorMap(mushroom.odor)
        val gillAttachment = gillAttachmentMap(mushroom.gillAttachment)
        val gillSpacing = gillSpacingMap(mushroom.gillSpacing)
        val gillSize = gillSizeMap(mushroom.gillSize)
        val gillColor = gillColorMap(mushroom.gillColor)
        val stalkShape = stalkShapeMap(mushroom.stalkShape)
        val stalkRoot = stalkRootMap(mushroom.stalkRoot)
        val stalkSurfaceAboveRing = stalkSurfaceAboveRingMap(mushroom.stalkSurfaceAboveRing)
        val stalkSurfaceBelowRing = stalkSurfaceBelowRingMap(mushroom.stalkSurfaceBelowRing)
        val stalkColorAboveRing = stalkColorAboveRingMap(mushroom.stalkColorAboveRing)
        val stalkColorBelowRing = stalkColorBelowRingMap(mushroom.stalkColorBelowRing)
        val veilType = veilTypeMap(mushroom.veilType)
        val veilColor = veilColorMap(mushroom.veilColor)
        val ringNumber = ringNumberMap(mushroom.ringNumber)
        val ringType = ringTypeMap(mushroom.ringType)
        val sporePrintColor = sporePrintColorMap(mushroom.sporePrintColor)
        val population = populationMap(mushroom.population)
        val habitat = habitatMap(mushroom.habitat)

        Array(edible.toDouble,capShape.toDouble,capSurface.toDouble,capColor.toDouble,bruises.toDouble,odor.toDouble,gillAttachment.toDouble,gillSpacing.toDouble,gillSize.toDouble,gillColor.toDouble,stalkShape.toDouble,stalkRoot.toDouble,stalkSurfaceAboveRing.toDouble,stalkSurfaceBelowRing.toDouble,stalkColorAboveRing.toDouble,stalkColorBelowRing.toDouble,veilType.toDouble,veilColor.toDouble,ringNumber.toDouble,ringType.toDouble,sporePrintColor.toDouble,population.toDouble,habitat.toDouble )
      }
    )

    preparedModel

  }

  def categoricalFeaturesInfo(): Map[Int,Int] = {

    var categoricalFeaturesMap: Map[Int, Int] = Map()

    categoricalFeaturesMap += (0-> capShapeMap.size)
    categoricalFeaturesMap += (1-> capSurfaceMap.size)
    categoricalFeaturesMap += (2-> capColorMap.size)
    categoricalFeaturesMap += (3-> bruisesMap.size)
    categoricalFeaturesMap += (4-> odorMap.size)
    categoricalFeaturesMap += (5-> gillAttachmentMap.size)
    categoricalFeaturesMap += (6-> gillSpacingMap.size)
    categoricalFeaturesMap += (7-> gillSizeMap.size)
    categoricalFeaturesMap += (8-> gillColorMap.size)
    categoricalFeaturesMap += (9-> stalkShapeMap.size)
    categoricalFeaturesMap += (10-> stalkRootMap.size)
    categoricalFeaturesMap += (11-> stalkSurfaceAboveRingMap.size)
    categoricalFeaturesMap += (12-> stalkSurfaceBelowRingMap.size)
    categoricalFeaturesMap += (13-> stalkColorAboveRingMap.size)
    categoricalFeaturesMap += (14-> stalkColorBelowRingMap.size)
    categoricalFeaturesMap += (15-> veilTypeMap.size)
    categoricalFeaturesMap += (16-> veilColorMap.size)
    categoricalFeaturesMap += (17-> ringNumberMap.size)
    categoricalFeaturesMap += (18-> ringTypeMap.size)
    categoricalFeaturesMap += (19-> sporePrintColorMap.size)
    categoricalFeaturesMap += (20-> populationMap.size)
    categoricalFeaturesMap += (21-> habitatMap.size)

    /*

    This is an example to remove some feature from features bin:

    categoricalFeaturesMap += (0-> capShapeMap.size)
    categoricalFeaturesMap += (1-> capSurfaceMap.size)
    categoricalFeaturesMap += (2-> capColorMap.size)
    categoricalFeaturesMap += (3-> bruisesMap.size)
    categoricalFeaturesMap += (4-> gillAttachmentMap.size)
    categoricalFeaturesMap += (5-> gillSpacingMap.size)
    categoricalFeaturesMap += (6-> gillSizeMap.size)
    categoricalFeaturesMap += (7-> gillColorMap.size)
    categoricalFeaturesMap += (8-> stalkShapeMap.size)
    categoricalFeaturesMap += (9-> stalkRootMap.size)
    categoricalFeaturesMap += (10-> stalkSurfaceAboveRingMap.size)
    categoricalFeaturesMap += (11-> stalkSurfaceBelowRingMap.size)
    categoricalFeaturesMap += (12-> stalkColorAboveRingMap.size)
    categoricalFeaturesMap += (13-> stalkColorBelowRingMap.size)
    categoricalFeaturesMap += (14-> veilTypeMap.size)
    categoricalFeaturesMap += (15-> veilColorMap.size)
    categoricalFeaturesMap += (16-> ringNumberMap.size)
    categoricalFeaturesMap += (17-> ringTypeMap.size)
    categoricalFeaturesMap += (18-> sporePrintColorMap.size)
    categoricalFeaturesMap += (19-> populationMap.size)
    categoricalFeaturesMap += (20-> habitatMap.size)

    */

    categoricalFeaturesMap

  }
}