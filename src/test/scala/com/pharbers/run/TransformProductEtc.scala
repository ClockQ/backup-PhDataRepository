package com.pharbers.run

import com.pharbers.util.log.phLogTrait.phDebugLog

object TransformProductEtc extends App {

    import com.pharbers.data.util._
    import com.pharbers.data.conversion._
    import org.apache.spark.sql.functions._
    import com.pharbers.data.util.ParquetLocation._


    val pfizer_source_id = "5ca069e2eeefcc012918ec73"
    val pfizer_cpa_csv = "/test/CPA&GYCX/Pfizer_201804_CPA_20181227.csv"
    val pfizer_gyc_csv = "/test/CPA&GYCX/Pfizer_201804_Gycx_20181127.csv"
    val pfizer_prod_match = "/data/pfizer/pha_config_repository1901/Pfizer_ProductMatchTable_20190403.csv"

    val pfizerProdCvs = ProductEtcConversion(pfizer_source_id)

    val productDevERD = Parquet2DF(PROD_DEV_LOCATION)

//    val pfizerCpaDF = CSV2DF(pfizer_cpa_csv).trim("SOURCE", "CPA").select("SOURCE", "PRODUCT_NAME", "MOLE_NAME", "PACK_DES", "PACK_NUMBER", "DOSAGE", "DELIVERY_WAY", "CORP_NAME")
//    val pfizerGycDF = CSV2DF(pfizer_gyc_csv).trim("SOURCE", "GYCX").select("SOURCE", "PRODUCT_NAME", "MOLE_NAME", "PACK_DES", "PACK_NUMBER", "DOSAGE", "DELIVERY_WAY", "CORP_NAME")
//    val pfizerProcMatch = CSV2DF(pfizer_prod_match)
//    val pfizerSource = pfizerCpaDF union pfizerGycDF
//
//    val pfizerProductEtcERD = pfizerProdCvs.toERD(
//        Map(
//            "sourceDataDF" -> pfizerSource,
//            "productDevERD" -> productDevERD,
//            "productMatchDF" -> pfizerProcMatch
//        )
//    )("productEtcERD")
//    //TODO：未匹配到标准形式的只存在公司维度，max计算中是不计这部分的。
//    val pfizerProdNotMatch = pfizerProductEtcERD.filter(col("PRODUCT_ID") === "")
//    phDebugLog("pfizerProdNotMatch count = " + pfizerProdNotMatch.count())  //935
//    pfizerProductEtcERD.save2Parquet(PROD_ETC_LOCATION + "/" + pfizer_source_id)
//    pfizerProductEtcERD.save2Mongo(PROD_ETC_LOCATION.split("/").last)
//
//    val nhwa_source_id = "5ca069bceeefcc012918ec72"
//    val nhwa_cpa_csv = "/test/CPA&GYCX/Nhwa_201804_CPA_20181227.csv"
//    val nhwa_prod_match = "/data/nhwa/pha_config_repository1809/Nhwa_ProductMatchTable_20181126.csv"
//    val nhwaCpaDF = CSV2DF(nhwa_cpa_csv).trim("SOURCE", "CPA")
//    val nhwaProcMatch = CSV2DF(nhwa_prod_match)
//    val nhwaProdCvs = ProductEtcConversion(nhwa_source_id)
//    val nhwaProductEtcERD = nhwaProdCvs.toERD(
//        Map(
//            "sourceDataDF" -> nhwaCpaDF,
//            "productDevERD" -> productDevERD,
//            "productMatchDF" -> nhwaProcMatch.withColumnRenamed("PACK_COUNT", "PACK_NUMBER")
//        )
//    )("productEtcERD")
//    //TODO：未匹配到标准形式的只存在公司维度，max计算中是不计这部分的。
//    val nhwaProdNotMatch = nhwaProductEtcERD.filter(col("PRODUCT_ID") === "")
//    phDebugLog("nhwaProdNotMatch count = " + nhwaProdNotMatch.count())  //0
//    nhwaProductEtcERD.save2Parquet(PROD_ETC_LOCATION + "/" + nhwa_source_id)
//    nhwaProductEtcERD.save2Mongo(PROD_ETC_LOCATION.split("/").last)
//
//    val prodEtcMongoDF = Mongo2DF(PROD_ETC_LOCATION.split("/").last)
//    phDebugLog("prodEtcMongoDF count = " + prodEtcMongoDF.count())
//    phDebugLog("prodEtcMongoDF contrast `ERD` = " + (prodEtcMongoDF.count() == nhwaProductEtcERD.count() + pfizerProductEtcERD.count()))

    val pfizerProdDIS = pfizerProdCvs.toDIS(Map(
        "prodEtcERD" -> Parquet2DF(PROD_ETC_LOCATION + "/" + pfizer_source_id)
        , "atcERD" -> ???
    ))
}