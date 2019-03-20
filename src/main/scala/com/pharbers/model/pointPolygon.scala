package com.pharbers.model

case class pointPolygon(coordinates: Seq[String], `type`: String = "Point") {
    override def toString: String = {
        val coordinatesString = coordinates.mkString("[", ",", "]")
         "{type:" + `type` + "," + "coordinates:" + coordinatesString + "}"
    }
}
