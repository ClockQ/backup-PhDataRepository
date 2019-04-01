package com.pharbers.model

case class chcData(
                  packId: String,
                  city: String,
                  date: String,
                  atc3: String,
                  oadType: String,
                  moleName: String,
                  moleNameCN: String,
                  prodName: String,
                  pack: String,
                  manufacture: String,
                  sales: String,
                  units: String,
                  var chc_id: String = "",
                  var revenue_id: String = "",
                  var date_id: String = "",
                  var city_id: String = "",
                  var product_id: String = "",
                  var pack_id: String = "",
                  var oad_id: String = "",
                  var mole_id: String = "",
                  var manufacture_id: String = ""
                  ) {

}
