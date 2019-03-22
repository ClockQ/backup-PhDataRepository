package com.pharbers.model

case class polygon(coordinates: Seq[Seq[String]], `type`: String = "Polygon") {
    override def toString: String = {
        val coordinatesString = coordinates.mkString("[", ",", "]")
        "{type:" + `type` + "," + "coordinates:" + coordinatesString + "}"
    }
}