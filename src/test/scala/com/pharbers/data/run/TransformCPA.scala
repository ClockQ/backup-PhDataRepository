package com.pharbers.data.run

import com.pharbers.pactions.actionbase.{DFArgs, MapArgs}
import com.pharbers.util.log.phLogTrait.phDebugLog
import org.apache.spark.sql.DataFrame

object TransformCPA extends App {

    import com.pharbers.data.util._
    import com.pharbers.data.conversion._
    import org.apache.spark.sql.functions._
    import com.pharbers.data.util.ParquetLocation._
    import com.pharbers.data.util.sparkDriver.ss.implicits._

    val hospCvs = HospConversion()
    val prodCvs = ProductDevConversion()
    //(ProductImsConversion(), ProductEtcConversion())
    val cpaCvs = CPAConversion()(ProductEtcConversion())

    val phaDF = Parquet2DF(HOSP_PHA_LOCATION)
    val hospDIS = hospCvs.toDIS(MapArgs(Map(
        "hospBaseERD" -> DFArgs(Parquet2DF(HOSP_BASE_LOCATION))
        , "hospAddressERD" -> DFArgs(Parquet2DF(HOSP_ADDRESS_BASE_LOCATION))
        , "hospPrefectureERD" -> DFArgs(Parquet2DF(HOSP_ADDRESS_PREFECTURE_LOCATION))
        , "hospCityERD" -> DFArgs(Parquet2DF(HOSP_ADDRESS_CITY_LOCATION))
        , "hospProvinceERD" -> DFArgs(Parquet2DF(HOSP_ADDRESS_PROVINCE_LOCATION))
    ))).getAs[DFArgs]("hospDIS")

    def nhwaCpaERD(): Unit = {
        val nhwa_source_id = NHWA_COMPANY_ID

        val nhwa_cpa_csv = "/test/CPA&GYCX/Nhwa_201804_CPA_20181227.csv"

        val cpaDF = CSV2DF(nhwa_cpa_csv)

        val nhwaProductDIS: DataFrame = ???
//        prodCvs.toDIS(Map(
//            "productDevERD" -> Parquet2DF(PROD_DEV_LOCATION)
//            , "productEtcERD" -> Parquet2DF(PROD_ETC_LOCATION + "/" + nhwa_source_id)
//        ))("productDIS")
//        nhwaProductDIS.show(false)

        val nhwaResult = cpaCvs.toERD(Map(
            "cpaDF" -> cpaDF.withColumn("COMPANY_ID", lit(nhwa_source_id))
            , "hospDF" -> hospDIS
            , "prodDF" -> nhwaProductDIS
            , "phaDF" -> phaDF
        ))

        val nhwaERD = nhwaResult("cpaERD")
        val nhwaProd = nhwaResult("prodDIS")
        val nhwaHosp = nhwaResult("hospDIS")
        val nhwaPha = nhwaResult("phaDIS")
        phDebugLog("nhwaERD", cpaDF.count(), nhwaERD.count())
        phDebugLog("nhwaProd", nhwaProductDIS.count(), nhwaProd.count())
        phDebugLog("nhwaHosp", hospDIS.count(), nhwaHosp.count())
        phDebugLog("nhwaPha", phaDF.count(), nhwaPha.count())
        val nhwaMinus = cpaDF.count() - nhwaERD.count()
        assert(nhwaMinus == 0, "nhwa: 转换后的ERD比源数据减少`" + nhwaMinus + "`条记录")


    }

    nhwaCpaERD()

    def pfizerCpaERD(): Unit = {
        val pfizer_source_id = PFIZER_COMPANY_ID

        val pfizer_cpa_csv = "/test/CPA&GYCX/Pfizer_201804_CPA_20181227.csv"

        val cpaDF = CSV2DF(pfizer_cpa_csv)

        val pfizerProductDIS: DataFrame = ???
//        prodCvs.toDIS(Map(
//            "productDevERD" -> Parquet2DF(PROD_DEV_LOCATION)
//            , "productEtcERD" -> Parquet2DF(PROD_ETC_LOCATION + "/" + pfizer_cpa_csv)
//        ))("productDIS")

        val pfizerResult = cpaCvs.toERD(Map(
            "cpaDF" -> cpaDF.withColumn("COMPANY_ID", lit(pfizer_source_id))
            , "hospDF" -> hospDIS
            , "prodDF" -> pfizerProductDIS
            , "phaDF" -> phaDF
        ))

        val pfizerERD = pfizerResult("cpaERD")
        val pfizerProd = pfizerResult("prodDIS")
        val pfizerHosp = pfizerResult("hospDIS")
        val pfizerPha = pfizerResult("phaDIS")
        phDebugLog("pfizerERD", cpaDF.count(), pfizerERD.count())
        phDebugLog("pfizerProd", pfizerProductDIS.count(), pfizerProd.count())
        phDebugLog("pfizerHosp", hospDIS.count(), pfizerHosp.count())
        phDebugLog("pfizerPha", phaDF.count(), pfizerPha.count())
        val pfizerMinus = cpaDF.count() - pfizerERD.count()
        assert(pfizerMinus == 0, "pfizer: 转换后的ERD比源数据减少`" + pfizerMinus + "`条记录")

    }

    pfizerCpaERD()


//    cpaDF.save2Parquet(PFIZER_CPA_LOCATION)
//    cpaDF.save2Mongo(PFIZER_CPA_LOCATION.split("/").last)
//
//    revenueDF.save2Parquet(HOSP_REVENUE_LOCATION)
//    revenueDF.save2Mongo(HOSP_REVENUE_LOCATION.split("/").last)
//
//
//
//
//
//    val cpaMongoDF = Mongo2DF(PFIZER_CPA_LOCATION.split("/").last)
//    phDebugLog("cpaMongoDF `mongodb` count = " + cpaMongoDF.count())
//    phDebugLog("cpaMongoDF `mongodb` contrast `ERD` = " + (cpaMongoDF.count() == cpaDF.count()))
//
//    val revenueMongoDF = Mongo2DF(HOSP_REVENUE_LOCATION.split("/").last)
//    phDebugLog("revenueMongoDF `mongodb` count = " + revenueMongoDF.count())
//    phDebugLog("revenueMongoDF `mongodb` contrast `ERD` = " + (revenueMongoDF.count() == revenueDF.count()))
}