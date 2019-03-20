package com.pharbers.model

case class pointPolygon(coordinates: Seq[String]) {
    val `type`: String = "point"
    override def toString: String = {
        val coordinatesString = coordinates.mkString("[", ",", "]")
         "{type:" + `type` + "," + "coordinates:" + coordinatesString + "}"
    }
}
