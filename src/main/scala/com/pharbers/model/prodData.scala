package com.pharbers.model

case class prodData(
                       productName: String,
                       value: String,
                       standardUnit: String,

                       moleName: String,
                       packageDes: String,
                       packageNumber: String,
                       dosage: String,
                       deliveryWay: String,
                       corpName: String,

                       var productID:String = "",
                       var moleID:String = "",
                       var packageID: String = "",
                       var dosageID: String = "",
                       var deliveryID: String = "",
                       var corpID: String = ""
                   ) {
}
