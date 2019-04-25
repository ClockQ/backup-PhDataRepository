package com.pharbers.data.model.hosp

/**
  * @description:
  * @author: clock
  * @date: 2019-04-25 18:14
  */
case class gycData(
                       hospitalID: String,
                       productID: String,
                       value: String,
                       unit: String,
                       ym: String,
                       source: String,
                       tag: String,

                       var gycID: String = "",
                       var sourceID: String = "",
                       var saleInfoID: String = ""
                   ) {
}
