package com.pharbers.data.model.hosp

/**
  * @description:
  * @author: clock
  * @date: 2019-04-25 18:14
  */
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
