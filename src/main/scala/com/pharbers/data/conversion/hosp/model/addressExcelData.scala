package com.pharbers.data.conversion.hosp.model

case class addressExcelData(region: String,
                            location: String,
                            province: String,
                            city: String,
                            prefecture: String,
                            tier: String,
                            var addressID:String,
                            var prefectureID: String = "",
                            var cityID: String = "",
                            var provinceID: String = "",
                            var tierID: String = "",
                            var regionID: String = "",
                            var desc: String = "") {

}
