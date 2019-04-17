package com.pharbers.run

import com.pharbers.util.log.phLogTrait.phDebugLog

/**
  * @description:
  * @author: clock
  * @date: 2019-04-16 19:24
  */
object TransformCHC extends App {

    import com.pharbers.data.util._
    import com.pharbers.data.conversion._
    import com.pharbers.data.util.ParquetLocation._

    val chcFile = "/test/OAD CHC data for 5 cities to 2018Q3 v3.csv"

    val chcDF = CSV2DF(chcFile)
    val cityDF = Parquet2DF(HOSP_ADDRESS_CITY_LOCATION)
    chcDF.show(false)
    val pdc = ProductDevConversion()
    val chcCvs = CHCConversion()

    val productDIS = pdc.toDIS(Map(
        "productDevERD" -> Parquet2DF(PROD_DEV_LOCATION)
        , "productImsERD" -> Parquet2DF(PROD_IMS_LOCATION)
    ))("productDIS")

    val chcResult = chcCvs.toERD(Map(
        "chcDF" -> chcDF
        , "prodDF" -> productDIS
        , "cityDF" -> cityDF
    ))
    val chcERD = chcResult("chcERD")
//    chcERD.show(false)
    val dateERD = chcResult("dateERD")
//    dateERD.show(false)
    phDebugLog("chcERD", chcDF.count(), chcERD.count())
    phDebugLog("dateERD", 0, dateERD.count())

//    chcERD.save2Parquet(CHC_LOCATION)
//    chcERD.save2Mongo(CHC_LOCATION.split("/").last)
//    dateERD.save2Parquet(CHC_DATE_LOCATION)
//    dateERD.save2Mongo(CHC_DATE_LOCATION.split("/").last)
}
