package model

case class addressData(region: String, location: String, province: String, city: String, prefecture: String, tier: Int,
                       var addressID:String = "", var prefectureID: String = "", var cityID: String = "", var provinceID: String = "", var tierID: String = "", var regionID: String = "" ) {

}
