package com.pharbers.data.conversion.hosp.model

import org.bson.types.ObjectId

case class cityData(_id: String, name: String, polygon: polygon, var tier: List[String], province:  String) {

}
