package com.pharbers.run

import com.pharbers.util.log.phLogTrait.phDebugLog

object TransformGYC extends App {

    import com.pharbers.data.util._
    import com.pharbers.data.conversion._
    import com.pharbers.data.util.ParquetLocation._

    val pfizer_source_id = "5ca069e2eeefcc012918ec73"
    val pfizer_gyc_csv = "/test/CPA&GYCX/Pfizer_201804_Gycx_20181127.csv"

    val hospCvs = HospConversion()
    val pfizerProdCvs = ProductEtcConversion(pfizer_source_id)
    val pfizerGycCvs = GYCConversion(pfizer_source_id)(pfizerProdCvs)

    val pfizerGycDF = CSV2DF(pfizer_gyc_csv)
    println("pfizerGycDF = " + pfizerGycDF.count())
    val phaDF = Parquet2DF(HOSP_PHA_LOCATION)
    println("phaDF = " + phaDF.count())

    val hospDIS = hospCvs.toDIS(
        Map(
            "hospBaseERD" -> Parquet2DF(HOSP_BASE_LOCATION),
            "hospBedERD" -> Parquet2DF(HOSP_BED_LOCATION),
            "hospEstimateERD" -> Parquet2DF(HOSP_ESTIMATE_LOCATION),
            "hospOutpatientERD" -> Parquet2DF(HOSP_OUTPATIENT_LOCATION),
            "hospRevenueERD" -> Parquet2DF(HOSP_REVENUE_LOCATION),
            "hospSpecialtyERD" -> Parquet2DF(HOSP_SPECIALTY_LOCATION),
            "hospStaffNumERD" -> Parquet2DF(HOSP_STAFFNUM_LOCATION),
            "hospUnitERD" -> Parquet2DF(HOSP_UNIT_LOCATION)
        )
    )("hospDIS")
    val prodDIS = pfizerProdCvs.toDIS(
        Map(
            "productEtcERD" -> Parquet2DF(PROD_ETC_LOCATION + "/" + pfizer_source_id),
            "productDevERD" -> Parquet2DF(PROD_DEV_LOCATION)
        )
    )("productEtcDIS")

    val pfizerERD = pfizerGycCvs.toERD(
        Map(
            "gycDF" -> pfizerGycDF,
            "hospDF" -> hospDIS,
            "prodDF" -> prodDIS,
            "phaDF" -> phaDF
        )
    )("gycERD")
//    val pfizerMinus = pfizerGycDF.count() - pfizerERD.count()
//    phDebugLog("pfizerERD count = " + pfizerERD.count())
//    assert(pfizerMinus == 0, "pfizer: 转换后的ERD比源数据减少`" + pfizerMinus + "`条记录")

//    val pfizerDIS = pfizerGycCvs.toDIS(
//        Map(
//            "gycERD" -> pfizerERD,
//            "hospERD" -> hospDIS,
//            "prodERD" -> prodDIS
//        )
//    )("gycDIS")
//    val pfizerDISMinus = pfizerGycDF.count() - pfizerDIS.count()
//    phDebugLog("pfizerDIS count = " + pfizerDIS.count())
//    assert(pfizerDISMinus == 0, "pfizer: 转换后的DIS比源数据减少`" + pfizerDISMinus + "`条记录")
//    pfizerDIS.show(true)

}
