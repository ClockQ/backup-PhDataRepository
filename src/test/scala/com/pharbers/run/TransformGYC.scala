package com.pharbers.run

import com.pharbers.util.log.phLogTrait.phDebugLog

object TransformGYC extends App {

    import com.pharbers.data.conversion._
    import com.pharbers.data.util.ParquetLocation._
    import com.pharbers.data.util._

    val pfizer_source_id = "5ca069e2eeefcc012918ec73"
    val astellas_source_id = "5ca069e5eeefcc012918ec74"
    val pfizer_gyc_csv = "/test/CPA&GYCX/Pfizer_201804_Gycx_20181127.csv"
    val astellas_gyc_csv = "/test/CPA&GYCX/Astellas_201804_Gycx_20180703.csv"
    val pha_csv = "/test/CPA&GYCX/CPA_GYC_PHA.csv"

    val hospCvs = HospConversion()
    val prodCvs = ProdConversion()
    val pfizerGycCvs = GYCConversion("pfizer", pfizer_source_id)
    val astellasGycCvs = GYCConversion("astellas", astellas_source_id)

    val pfizerGycDF = CSV2DF(pfizer_gyc_csv)
    val astellasGycDF = CSV2DF(astellas_gyc_csv)
    val phaDF = CSV2DF(pha_csv)

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
    val prodDIS = prodCvs.toDIS(
        Map(
            "prodBaseERD" -> Parquet2DF(PROD_BASE_LOCATION),
            "prodDeliveryERD" -> Parquet2DF(PROD_DELIVERY_LOCATION),
            "prodDosageERD" -> Parquet2DF(PROD_DOSAGE_LOCATION),
            "prodMoleERD" -> Parquet2DF(PROD_MOLE_LOCATION),
            "prodPackageERD" -> Parquet2DF(PROD_PACKAGE_LOCATION),
            "prodCorpERD" -> Parquet2DF(PROD_CORP_LOCATION)
        )
    )("prodDIS")

    val pfizerERD = pfizerGycCvs.toERD(
        Map(
            "gycDF" -> pfizerGycDF,
            "hospDF" -> hospDIS,
            "prodDF" -> prodDIS,
            "phaDF" -> phaDF
        )
    )("gycERD")
    val pfizerMinus = pfizerGycDF.count() - pfizerERD.count()
    phDebugLog("pfizerERD count = " + pfizerERD.count())
    assert(pfizerMinus == 0, "pfizer: 转换后的ERD比源数据减少`" + pfizerMinus + "`条记录")

    val astellasERD = astellasGycCvs.toERD(
        Map(
            "gycDF" -> astellasGycDF,
            "hospDF" -> hospDIS,
            "prodDF" -> prodDIS,
            "phaDF" -> phaDF
        )
    )("gycERD")
    val astellasMinus = astellasGycDF.count() - astellasERD.count()
    phDebugLog("astellasERD count = " + astellasERD.count())
    assert(astellasMinus == 0, "astellas: 转换后的ERD比源数据减少`" + astellasMinus + "`条记录")
    astellasERD.show(true)

    val astellasDIS = astellasGycCvs.toDIS(
        Map(
            "gycERD" -> astellasERD,
            "hospERD" -> hospDIS,
            "prodERD" -> prodDIS,
            "phaERD" -> phaDF
        )
    )("gycDIS")
    val astellasDISMinus = astellasGycDF.count() - astellasDIS.count()
    phDebugLog("astellasDIS count = " + astellasDIS.count())
    assert(astellasMinus == 0, "astellas: 转换后的DIS比源数据减少`" + astellasMinus + "`条记录")
    astellasDIS.show(true)

}
