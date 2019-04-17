package com.pharbers.run

import com.pharbers.util.log.phLogTrait.phDebugLog

object TransformCPA extends App {

    import com.pharbers.data.util._
    import com.pharbers.data.conversion._
    import com.pharbers.data.util.ParquetLocation._

    val nhwa_source_id = "5ca069bceeefcc012918ec72"
    val pfizer_source_id = "5ca069e2eeefcc012918ec73"
    val nhwa_cpa_csv = "/test/CPA&GYCX/Nhwa_201804_CPA_20181227.csv"
    val pfizer_cpa_csv = "/test/CPA&GYCX/Pfizer_201804_CPA_20181227.csv"

    val hospCvs = HospConversion()
    val nhwaProdCvs = ProductEtcConversion(nhwa_source_id)
    val pfizerProdCvs = ProductEtcConversion(nhwa_source_id)
    val nhwaCpaCvs = CPAConversion(nhwa_source_id)(nhwaProdCvs)
    val pfizerCpaCvs = CPAConversion(pfizer_source_id)(pfizerProdCvs)

    val nhwaCpaDF = CSV2DF(nhwa_cpa_csv)
    val pfizerCpaDF = CSV2DF(pfizer_cpa_csv)
    val phaDF = Parquet2DF(HOSP_PHA_LOCATION)

    val hospDIS = hospCvs.toDIS(
        Map(
            "hospBaseERD" -> Parquet2DF(HOSP_BASE_LOCATION),
            "hospAddressERD" -> Parquet2DF(HOSP_ADDRESS_BASE_LOCATION),
            "hospPrefectureERD" -> Parquet2DF(HOSP_ADDRESS_PREFECTURE_LOCATION),
            "hospCityERD" -> Parquet2DF(HOSP_ADDRESS_CITY_LOCATION),
            "hospProvinceERD" -> Parquet2DF(HOSP_ADDRESS_PROVINCE_LOCATION)
        )
    )("hospDIS")
//    val nhwaProdDIS = nhwaProdCvs.toDIS(
//        Map(
//            "productEtcERD" -> Parquet2DF(PROD_ETC_LOCATION + "/" + nhwa_source_id),
//            "productDevERD" -> Parquet2DF(PROD_DEV_LOCATION),
//            "productImsERD" -> Parquet2DF(PROD_IMS_LOCATION)
//        )
//    )("productEtcDIS")
    val pfizerProdDIS = pfizerProdCvs.toDIS(
        Map(
            "productEtcERD" -> Parquet2DF(PROD_ETC_LOCATION + "/" + pfizer_source_id),
            "productDevERD" -> Parquet2DF(PROD_DEV_LOCATION),
            "productImsERD" -> Parquet2DF(PROD_IMS_LOCATION)
        )
    )("productEtcDIS")

//    val nhwaResult = nhwaCpaCvs.toERD(
//        Map(
//            "cpaDF" -> nhwaCpaDF,
//            "hospDF" -> hospDIS,
//            "prodDF" -> nhwaProdDIS,
//            "phaDF" -> phaDF
//        )
//    )
//    val nhwaERD = nhwaResult("cpaERD")
//    val nhwaProd = nhwaResult("prodDIS")
//    val nhwaHosp = nhwaResult("hospDIS")
//    val nhwaPha = nhwaResult("phaDIS")
//    phDebugLog("nhwaERD", nhwaCpaDF.count(), nhwaERD.count())
//    phDebugLog("nhwaProd", nhwaProdDIS.count(), nhwaProd.count())
//    phDebugLog("nhwaHosp", hospDIS.count(), nhwaHosp.count())
//    phDebugLog("nhwaPha", phaDF.count(), nhwaPha.count())
//    val nhwaMinus = nhwaCpaDF.count() - nhwaERD.count()
//    assert(nhwaMinus == 0, "nhwa: 转换后的ERD比源数据减少`" + nhwaMinus + "`条记录")

    val pfizerResult = pfizerCpaCvs.toERD(
        Map(
            "cpaDF" -> pfizerCpaDF,
            "hospDF" -> hospDIS,
            "prodDF" -> pfizerProdDIS,
            "phaDF" -> phaDF
        )
    )
    val pfizerERD = pfizerResult("cpaERD")
    val pfizerProd = pfizerResult("prodDIS")
    val pfizerHosp = pfizerResult("hospDIS")
    val pfizerPha = pfizerResult("phaDIS")
    phDebugLog("pfizerERD", pfizerCpaDF.count(), pfizerERD.count()) //==default=> (pfizerERD,178485,204833)
    phDebugLog("pfizerProd", pfizerProdDIS.count(), pfizerProd.count())
    phDebugLog("pfizerHosp", hospDIS.count(), pfizerHosp.count())
    phDebugLog("pfizerPha", phaDF.count(), pfizerPha.count())
    val pfizerMinus = pfizerCpaDF.count() - pfizerERD.count()
    assert(pfizerMinus == 0, "pfizer: 转换后的ERD比源数据减少`" + pfizerMinus + "`条记录")

//    cpaDF.save2Parquet(PFIZER_CPA_LOCATION)
//    cpaDF.save2Mongo(PFIZER_CPA_LOCATION.split("/").last)
//
//    revenueDF.save2Parquet(HOSP_REVENUE_LOCATION)
//    revenueDF.save2Mongo(HOSP_REVENUE_LOCATION.split("/").last)
//
//    val cpaMongoDF = Mongo2DF(PFIZER_CPA_LOCATION.split("/").last)
//    phDebugLog("cpaMongoDF `mongodb` count = " + cpaMongoDF.count())
//    phDebugLog("cpaMongoDF `mongodb` contrast `ERD` = " + (cpaMongoDF.count() == cpaDF.count()))
//
//    val revenueMongoDF = Mongo2DF(HOSP_REVENUE_LOCATION.split("/").last)
//    phDebugLog("revenueMongoDF `mongodb` count = " + revenueMongoDF.count())
//    phDebugLog("revenueMongoDF `mongodb` contrast `ERD` = " + (revenueMongoDF.count() == revenueDF.count()))
}