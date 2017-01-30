package com.sircamp.models

/**
  * Created by stefano on 27/01/17.
  */
case class Mushroom (edible: String, capShape: String, capSurface: String, capColor: String, bruises: String, odor: String, gillAttachment: String, gillSpacing: String, gillSize: String, gillColor: String, stalkShape: String, stalkRoot: String, stalkSurfaceAboveRing: String, stalkSurfaceBelowRing: String, stalkColorAboveRing: String, stalkColorBelowRing: String, veilType: String, veilColor: String, ringNumber: String, ringType: String, sporePrintColor: String, population: String, habitat:String)


object Mushroom {

  def parseMushroom(str: String): Mushroom = {
    val line = str.split(",")

    Mushroom(line(0), line(1),line(2),line(3),line(4),line(5),line(6),line(7),line(8),line(9),line(10),line(11),line(12),line(13),line(14),line(15),line(16),line(17),line(18),line(19),line(20),line(21),line(22))

  }
}
