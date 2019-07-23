package com.pharbers.data.model.hosp

/**
  * @description:
  * @author: clock
  * @date: 2019-04-25 18:14
  */
case class pointPolygon(coordinates: Seq[String], `type`: String = "Point") {
    override def toString: String = {
        val coordinatesString = coordinates.mkString("[", ",", "]")
         "{type:" + `type` + "," + "coordinates:" + coordinatesString + "}"
    }
}
