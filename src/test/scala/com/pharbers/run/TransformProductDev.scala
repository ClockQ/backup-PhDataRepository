package com.pharbers.run

/**
  * @description:
  * @author: clock
  * @date: 2019-04-16 17:50
  */
object TransformProductDev extends App {
    import com.pharbers.data.util._
    import com.pharbers.data.conversion._
    import com.pharbers.data.util.ParquetLocation._

    val nhwaProductMatchFile = "/data/nhwa/pha_config_repository1809/Nhwa_ProductMatchTable_20181126.csv"
    val pfizerProductMatchFile = "/data/pfizer/pha_config_repository1901/Pfizer_ProductMatchTable_20190403.csv"

    val nhwaMatchDF = CSV2DF(nhwaProductMatchFile)
//    nhwaMatchDF.show(false)

    val pfizerMatchDF = CSV2DF(pfizerProductMatchFile)
//    pfizerMatchDF.show(false)

    val pdc = ProductDevConversion()
    val productDevERD = pdc.toERD(Map(
        "nhwaMatchDF" -> nhwaMatchDF.withColumnRenamed("PACK_COUNT", "PACK_NUMBER")
        , "pfizerMatchDF" -> pfizerMatchDF
    ))("productDevERD")
    productDevERD.show(false)
    println(productDevERD.count())
//    productDevERD.save2Mongo("prod-dev")
//    productDevERD.save2Parquet(PROD_DEV_LOCATION)

    val productImsERDArgs = Parquet2DF(PROD_IMS_LOCATION)
//    println(productImsERDArgs.count())
    val productDevERDArgs = Parquet2DF(PROD_DEV_LOCATION)
//    println(productDevERDArgs.count())

    val productDIS = pdc.toDIS(Map(
        "productDevERD" -> productDevERDArgs
        , "productImsERD" -> productImsERDArgs
    ))("productDIS")
    productDIS.show(false)
    println(productDIS.count())
}
