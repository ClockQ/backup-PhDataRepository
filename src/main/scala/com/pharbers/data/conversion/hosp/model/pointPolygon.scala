package com.pharbers.data.conversion.hosp.model

case class pointPolygon(coordinates: Seq[String], `type`: String = "Point") {
    override def toString: String = {
        val coordinatesString = coordinates.mkString("[", ",", "]")
         "{type:" + `type` + "," + "coordinates:" + coordinatesString + "}"
    }
}
