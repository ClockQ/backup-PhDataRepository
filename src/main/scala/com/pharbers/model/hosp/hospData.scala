package com.pharbers.model.hosp


case class hospData(_id: String, title: String, PHAHospId: String,`type`: String, level: String, character: String, addressID: String, var nos: List[String] = Nil, var estimates: List[String] = Nil,
                    var noo: List[String] = Nil, var nobs: List[String] = Nil, var revenues: List[String] = Nil, var specialty: List[String] = Nil) {


    override def toString: String = {
        _id + "," + title + "," + PHAHospId + "," + `type` + "," + level + "," + character + "," + addressID + "," + nobs.mkString("[", "*", "]") + "," +
                estimates.mkString("[", "*", "]") + "," + noo.mkString("[", "*", "]") + "," +
                nobs.mkString("[", "*", "]") + "," + revenues.mkString("[", "*", "]") + "," + specialty.mkString("[", "*", "]")
    }

}

//case class hospData(_id: String) {
//
//}


