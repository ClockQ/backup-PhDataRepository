package com.pharbers.run

import com.pharbers.util.log.phLogTrait.phDebugLog

object TransformPfizerCPA extends App {

    import com.pharbers.data.util._
    import com.pharbers.data.conversion._
    import com.pharbers.data.util.ParquetLocation._

    val cpa_csv = "/test/CPA&GYCX/Pfizer_201804_CPA_20181227.csv"

    val hospCvs = HospConversion()
    val prodCvs = ProdConversion()
    val cpaCvs = CPAConversion()

    val pfizerCpaDF = CSV2DF(cpa_csv)

    val hospDF = hospCvs.ERD2DF(
        Map(
            "hospBaseDF" -> Parquet2DF(HOSP_BASE_LOCATION),
            "hospBedDF" -> Parquet2DF(HOSP_BED_LOCATION),
            "hospEstimateDF" -> Parquet2DF(HOSP_ESTIMATE_LOCATION),
            "hospOutpatientDF" -> Parquet2DF(HOSP_OUTPATIENT_LOCATION),
            "hospRevenueDF" -> Parquet2DF(HOSP_REVENUE_LOCATION),
            "hospSpecialtyDF" -> Parquet2DF(HOSP_SPECIALTY_LOCATION),
            "hospStaffNumDF" -> Parquet2DF(HOSP_STAFFNUM_LOCATION),
            "hospUnitDF" -> Parquet2DF(HOSP_UNIT_LOCATION)
        )
    )("hospDF")
    hospDF.show(true)

    val prodDF = prodCvs.ERD2DF(
        Map(
            "prodBaseDF" -> Parquet2DF(PROD_BASE_LOCATION),
            "prodDeliveryDF" -> Parquet2DF(PROD_DELIVERY_LOCATION),
            "prodDosageDF" -> Parquet2DF(PROD_DOSAGE_LOCATION),
            "prodMoleDF" -> Parquet2DF(PROD_MOLE_LOCATION),
            "prodPackageDF" -> Parquet2DF(PROD_PACKAGE_LOCATION),
            "prodCorpDF" -> Parquet2DF(PROD_CORP_LOCATION)
        )
    )("prodDF")

    val result = cpaCvs.DF2ERD(
        Map(
            "cpaDF" -> pfizerCpaDF,
            "hospDF" -> hospDF,
            "prodDF" -> prodDF
        )
    )

    val cpaDF = result("cpaDF")
    val revenueDF = result("revenueDF")

    phDebugLog("cpaDF coount = " + cpaDF.count())
    cpaDF.show(true)

    phDebugLog("revenueDF coount = " + revenueDF.count())
    revenueDF.show(true)

    cpaDF.save2Parquet(PFIZER_CPA_LOCATION)
    cpaDF.save2Mongo(PFIZER_CPA_LOCATION.split("/").last)

    revenueDF.save2Parquet(HOSP_REVENUE_LOCATION)
    revenueDF.save2Mongo(HOSP_REVENUE_LOCATION.split("/").last)

    val cpaMongoDF = Mongo2DF(PFIZER_CPA_LOCATION.split("/").last)
    phDebugLog("cpaMongoDF mongodb count = " + cpaMongoDF.count())

    val revenueMongoDF = Mongo2DF(HOSP_REVENUE_LOCATION.split("/").last)
    phDebugLog("revenueMongoDF mongodb count = " + revenueMongoDF.count())
}