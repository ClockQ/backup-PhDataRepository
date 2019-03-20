package com.pharbers.model

import org.bson.types.ObjectId

case class cityData(_id: String, name: String, polygon: String, var tier: List[String], province:  String) {

}
