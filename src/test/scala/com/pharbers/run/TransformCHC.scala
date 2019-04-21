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
    val chcDFCount = chcDF.count()
    val cityDF = Parquet2DF(HOSP_ADDRESS_CITY_LOCATION)

    val pdc = ProductDevConversion()(ProductImsConversion(), ProductEtcConversion())
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
    val chcERDCount = chcERD.count()
//    chcERD.show(false)
    val dateERD = chcResult("dateERD")
//    dateERD.show(false)
    phDebugLog("chcERD", chcDFCount, chcERDCount)
    phDebugLog("dateERD", 0, dateERD.count())

    val chcMinus = chcDFCount - chcERDCount
    assert(chcMinus == 0, "chc: 转换后的ERD比源数据减少`" + chcMinus + "`条记录")
    val chcProdIsNullCount = chcERD.filter("PRODUCT_ID = null").count()
    assert(chcProdIsNullCount == 0, "chc: 转换后的ERD有`" + chcProdIsNullCount + "`条产品未匹配")

//    chcERD.save2Parquet(CHC_LOCATION)
//    chcERD.save2Mongo(CHC_LOCATION.split("/").last)
//    dateERD.save2Parquet(CHC_DATE_LOCATION)
//    dateERD.save2Mongo(CHC_DATE_LOCATION.split("/").last)

    val chcDIS = chcCvs.toDIS(Map(
        "chcERD" -> Parquet2DF(CHC_LOCATION)
        , "dateERD" -> Parquet2DF(CHC_DATE_LOCATION)
        , "cityERD" -> Parquet2DF(HOSP_ADDRESS_CITY_LOCATION)
        , "oadERD" -> Parquet2DF(PROD_OADTABLE_LOCATION)
        , "atc3ERD" -> Parquet2DF(PROD_ATC3TABLE_LOCATION)
        , "productDIS" -> productDIS
    ))("chcDIS")
//    chcDIS.show(false)
    chcCvs.toCHCStruct(chcDIS).show(false)
    phDebugLog("chcDIS", chcDF.count(), chcDIS.count())
}
