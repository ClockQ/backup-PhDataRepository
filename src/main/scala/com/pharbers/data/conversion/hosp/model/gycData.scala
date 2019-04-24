package com.pharbers.data.conversion.hosp.model

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
