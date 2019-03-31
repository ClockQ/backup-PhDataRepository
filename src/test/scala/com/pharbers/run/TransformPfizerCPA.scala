package com.pharbers.run

import org.apache.spark.sql.functions._
import com.pharbers.util.log.phLogTrait.phDebugLog

object TransformPfizerCPA extends App {

    import com.pharbers.data.util._
    import com.pharbers.data.conversion._
    import com.pharbers.data.util.ParquetLocation._

    val company = "pfizer"
    val source_id = "5ca069e2eeefcc012918ec73"
    val cpa_csv = "file:///Users/clock/Downloads/Pfizer_201804_CPA_20181227.csv"
    val pha_csv = "file:///Users/clock/Downloads/CPA_GYC_PHA.csv"

    val hospCvs = HospConversion()
    val prodCvs = ProdConversion()
    val cpaCvs = CPAConversion(company, source_id)

    val pfizerCpaDF = CSV2DF(cpa_csv)
//    pfizerCpaDF.show(true)

    val phaDF = CSV2DF(pha_csv)
//    phaDF.show(true)

    //    val hospDF = hospCvs.ERD2DF(
//        Map(
//            "hospBaseDF" -> Parquet2DF(HOSP_BASE_LOCATION),
//            "hospBedDF" -> Parquet2DF(HOSP_BED_LOCATION),
//            "hospEstimateDF" -> Parquet2DF(HOSP_ESTIMATE_LOCATION),
//            "hospOutpatientDF" -> Parquet2DF(HOSP_OUTPATIENT_LOCATION),
//            "hospRevenueDF" -> Parquet2DF(HOSP_REVENUE_LOCATION),
//            "hospSpecialtyDF" -> Parquet2DF(HOSP_SPECIALTY_LOCATION),
//            "hospStaffNumDF" -> Parquet2DF(HOSP_STAFFNUM_LOCATION),
//            "hospUnitDF" -> Parquet2DF(HOSP_UNIT_LOCATION)
//        )
//    )("hospDF")
val hospDF = hospCvs.toDIS(
    Map(
        "hospBaseDF" -> Mongo2DF(HOSP_BASE_LOCATION.split("/").last),
        "hospBedDF" -> Mongo2DF(HOSP_BED_LOCATION.split("/").last),
        "hospEstimateDF" -> Mongo2DF(HOSP_ESTIMATE_LOCATION.split("/").last),
        "hospOutpatientDF" -> Mongo2DF(HOSP_OUTPATIENT_LOCATION.split("/").last),
        "hospRevenueDF" -> Mongo2DF(HOSP_REVENUE_LOCATION.split("/").last),
        "hospSpecialtyDF" -> Mongo2DF(HOSP_SPECIALTY_LOCATION.split("/").last),
        "hospStaffNumDF" -> Mongo2DF(HOSP_STAFFNUM_LOCATION.split("/").last),
        "hospUnitDF" -> Mongo2DF(HOSP_UNIT_LOCATION.split("/").last)
    )
)("hospDF")
//    hospDF.show(true)

    //    val prodDF = prodCvs.ERD2DF(
//        Map(
//            "prodBaseDF" -> Parquet2DF(PROD_BASE_LOCATION),
//            "prodDeliveryDF" -> Parquet2DF(PROD_DELIVERY_LOCATION),
//            "prodDosageDF" -> Parquet2DF(PROD_DOSAGE_LOCATION),
//            "prodMoleDF" -> Parquet2DF(PROD_MOLE_LOCATION),
//            "prodPackageDF" -> Parquet2DF(PROD_PACKAGE_LOCATION),
//            "prodCorpDF" -> Parquet2DF(PROD_CORP_LOCATION)
//        )
//    )("prodDF")
val prodDF = prodCvs.toDIS(
    Map(
        "prodBaseDF" -> Mongo2DF(PROD_BASE_LOCATION.split("/").last),
        "prodDeliveryDF" -> Mongo2DF(PROD_DELIVERY_LOCATION.split("/").last),
        "prodDosageDF" -> Mongo2DF(PROD_DOSAGE_LOCATION.split("/").last),
        "prodMoleDF" -> Mongo2DF(PROD_MOLE_LOCATION.split("/").last),
        "prodPackageDF" -> Mongo2DF(PROD_PACKAGE_LOCATION.split("/").last),
        "prodCorpDF" -> Mongo2DF(PROD_CORP_LOCATION.split("/").last)
    )
)("prodDF")
//    prodDF.show(true)

    val result = cpaCvs.toERD(
        Map(
            "cpaDF" -> pfizerCpaDF,
            "hospDF" -> hospDF,
            "prodDF" -> prodDF,
            "phaDF" -> phaDF
        )
    )

    val cpaDF = result("cpaDF")
    val revenueDF = result("revenueDF")
    cpaDF.show(true)
    revenueDF.show(true)

    val notConnProdOfCpaCount = cpaDF.filter(col("product-id").isNull).count
    assert(notConnProdOfCpaCount == 0, notConnProdOfCpaCount + "条产品未匹配")

    val notConnHospOfCpaCount = cpaDF.filter(col("hosp-id").isNull).count
    assert(notConnHospOfCpaCount == 0, notConnHospOfCpaCount + "医院未匹配")

    phDebugLog("cpaDF `ERD` count = " + cpaDF.count())
    phDebugLog("revenueDF `ERD` count = " + revenueDF.count())

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