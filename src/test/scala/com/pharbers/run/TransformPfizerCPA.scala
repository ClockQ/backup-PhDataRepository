package com.pharbers.run

import com.pharbers.util.log.phLogTrait.phDebugLog

object TransformPfizerCPA extends App {

    import com.pharbers.data.util._
    import com.pharbers.data.conversion._
    import com.pharbers.data.util.ParquetLocation._

    val cpa_csv = "/test/CPA&GYCX/Pfizer_201804_CPA_20181227.csv"

    val hospCvs = HospConversion()
    val prodCvs = ProdConversion()

    val pfizerCpaDF = CSV2DF(cpa_csv)

    val hospDF = hospCvs.ERD2DF(
        Map(
            "prodBaseDF" -> Parquet2DF(PROD_BASE_LOCATION),
            "prodDeliveryDF" -> Parquet2DF(PROD_DELIVERY_LOCATION),
            "prodDosageDF" -> Parquet2DF(PROD_DOSAGE_LOCATION),
            "prodMoleDF" -> Parquet2DF(PROD_MOLE_LOCATION),
            "prodPackageDF" -> Parquet2DF(PROD_PACKAGE_LOCATION),
            "prodCorpDF" -> Parquet2DF(PROD_CORP_LOCATION)
        )
    )("hospDF")
    hospDF.show(true)

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

//    phDebugLog("atcTableDF coount = " + atcTableDF.count())
//    atcTableDF.show(true)
//
//    atcTableDF.save2Parquet(PROD_ATCTABLE_LOCATION)
//    atcTableDF.save2Mongo(PROD_ATCTABLE_LOCATION.split("/").last)
//
//    val atcTableMongoDF = Mongo2DF(PROD_ATCTABLE_LOCATION.split("/").last)
//    phDebugLog("atcTableDF mongodb coount = " + atcTableMongoDF.count())
}