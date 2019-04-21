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

    val pdc = ProductDevConversion()(ProductImsConversion(), ProductEtcConversion())

//    val nhwaMatchDF = CSV2DF(nhwaProductMatchFile)
//    val pfizerMatchDF = CSV2DF(pfizerProductMatchFile)
//    val chcDF = CSV2DF(chcFile)
//
//    val productDevERD = pdc.toERD(Map(
//        "nhwaMatchDF" -> pdc.matchTable2Product(nhwaMatchDF.withColumnRenamed("PACK_COUNT", "PACK_NUMBER"))
//        , "pfizerMatchDF" -> pdc.matchTable2Product(pfizerMatchDF)
//        , "chcDF" -> pdc.chc2Product(chcDF)
//    ))("productDevERD")
//    productDevERD.show(false)
//    println(productDevERD.count())
//
//    if (args.isEmpty || args(0) == "TRUE") {
//        productDevERD.save2Mongo(PROD_DEV_LOCATION.split("/").last)
//        productDevERD.save2Parquet(PROD_DEV_LOCATION)
//    }

    val productImsERDArgs = Parquet2DF(PROD_IMS_LOCATION) // 112848
//    println(productImsERDArgs.count())
    val productDevERDArgs = Parquet2DF(PROD_DEV_LOCATION) // 17765
//    println(productDevERDArgs.count())
    val nhwaProductEtcERDArgs = Parquet2DF(PROD_ETC_LOCATION + "/" + NHWA_COMPANY_ID) // 128
//    println(nhwaProductEtcERDArgs.count())
    val pfizerProductEtcERDArgs = Parquet2DF(PROD_ETC_LOCATION + "/" + PFIZER_COMPANY_ID) // 13851
//    println(pfizerProductEtcERDArgs.count())

    val nhwaProductDIS = pdc.toDIS(Map(
        "productDevERD" -> productDevERDArgs
        , "productImsERD" -> productImsERDArgs
        , "atc3ERD" -> Parquet2DF(PROD_ATC3TABLE_LOCATION)
        , "oadERD" -> Parquet2DF(PROD_OADTABLE_LOCATION)
        , "productEtcERD" -> nhwaProductEtcERDArgs
        , "marketERD" -> Parquet2DF(PROD_MARKET_LOCATION + "/" + NHWA_COMPANY_ID)
        , "atcERD" -> Parquet2DF(PROD_ATCTABLE_LOCATION)
    ))("productDIS")
//    nhwaProductDIS.show(false)
    println("nhwa product ETC count: " + nhwaProductEtcERDArgs.count())
    println("nhwa product DIS count: " + nhwaProductDIS.count())

    val pfizerProductDIS = pdc.toDIS(Map(
        "productDevERD" -> productDevERDArgs
        , "productImsERD" -> productImsERDArgs
        , "atc3ERD" -> Parquet2DF(PROD_ATC3TABLE_LOCATION)
        , "oadERD" -> Parquet2DF(PROD_OADTABLE_LOCATION)
        , "productEtcERD" -> pfizerProductEtcERDArgs
        , "marketERD" -> Parquet2DF(PROD_MARKET_LOCATION + "/" + PFIZER_COMPANY_ID)
        , "atcERD" -> Parquet2DF(PROD_ATCTABLE_LOCATION)
    ))("productDIS")
//    pfizerProductDIS.show(false)
    println("pfizer product ETC count: " + pfizerProductEtcERDArgs.count())
    println("pfizer product DIS count: " + pfizerProductDIS.count())
}
