package com.pharbers.model

import org.bson.types.ObjectId

case class addressData(_id: String, location: pointPolygon, prefecture: String, region: List[String], var desc: String = "NA") {

}
