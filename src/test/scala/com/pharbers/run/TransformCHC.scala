package com.pharbers.run

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

    val pdc = ProductDevConversion()
    val chcCvs = CHCConversion()

    val productDIS = pdc.toDIS(Map(
        "productDevERD" -> Parquet2DF(PROD_DEV_LOCATION)
        , "productImsERD" -> Parquet2DF(PROD_IMS_LOCATION)
    ))("productDIS")

    val chcERD = chcCvs.toERD(Map(
        "chcDF" -> chcDF
        , "prodDF" -> productDIS
        , "cityDF" -> cityDF
    ))("chcERD")
    chcERD.show(false)

//    chcERD.save2Parquet(CHC_LOCATION)
}
