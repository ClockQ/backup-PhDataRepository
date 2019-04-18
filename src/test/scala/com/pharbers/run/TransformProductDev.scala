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
    val chcFile = "/test/OAD CHC data for 5 cities to 2018Q3 v3.csv"

    val nhwaMatchDF = CSV2DF(nhwaProductMatchFile)
//    nhwaMatchDF.show(false)

    val pfizerMatchDF = CSV2DF(pfizerProductMatchFile)
//    pfizerMatchDF.show(false)

    val chcDF = CSV2DF(chcFile)
//    chcDF.show(false)

    val pdc = ProductDevConversion()
    val productDevERD = pdc.toERD(Map(
        "nhwaMatchDF" -> pdc.matchTable2Product(nhwaMatchDF.withColumnRenamed("PACK_COUNT", "PACK_NUMBER"))
        , "pfizerMatchDF" -> pdc.matchTable2Product(pfizerMatchDF)
        , "chcDF" -> pdc.chc2Product(chcDF)
    ))("productDevERD")
    productDevERD.show(false)
    println(productDevERD.count())
    productDevERD.save2Mongo("prod_dev")
    productDevERD.save2Parquet(PROD_DEV_LOCATION)

    val productImsERDArgs = Parquet2DF(PROD_IMS_LOCATION) // 112848
//    println(productImsERDArgs.count())
    val productDevERDArgs = Parquet2DF(PROD_DEV_LOCATION) // 17765
//    println(productDevERDArgs.count())
    val productEtcERDArgs = Parquet2DF(PROD_ETC_LOCATION + "/5ca069bceeefcc012918ec72")
//    println(productEtcERDArgs.count())

    val productDIS = pdc.toDIS(Map(
        "productDevERD" -> productDevERDArgs
        , "productImsERD" -> productImsERDArgs
        , "productEtcERD" -> productEtcERDArgs
    ))("productDIS")
    productDIS.show(false)
    println(productDIS.count())
}
