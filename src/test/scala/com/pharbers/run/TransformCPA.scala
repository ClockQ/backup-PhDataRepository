package com.pharbers.run

import com.pharbers.util.log.phLogTrait.phDebugLog

object TransformCPA extends App {

    import com.pharbers.data.util._
    import com.pharbers.data.conversion._
    import com.pharbers.data.util.ParquetLocation._

    val nhwa_source_id = "5ca069bceeefcc012918ec72"
    val pfizer_source_id = "5ca069e2eeefcc012918ec73"
    val astellas_source_id = "5ca069e5eeefcc012918ec74"
    val nhwa_cpa_csv = "/test/CPA&GYCX/Nhwa_201804_CPA_20181227.csv"
    val pfizer_cpa_csv = "/test/CPA&GYCX/Pfizer_201804_CPA_20181227.csv"
    val astellas_cpa_csv = "/test/CPA&GYCX/Astellas_201804_CPA_20180629.csv"
    val pha_csv = "/test/CPA&GYCX/CPA_GYC_PHA.csv"

    val hospCvs = HospConversion()
    val prodCvs = ProdConversion()
    val nhwaCpaCvs = CPAConversion("nhwa", nhwa_source_id)
    val pfizerCpaCvs = CPAConversion("pfizer", pfizer_source_id)
    val astellasCpaCvs = CPAConversion("astellas", astellas_source_id)

    val nhwaCpaDF = CSV2DF(nhwa_cpa_csv)
    val pfizerCpaDF = CSV2DF(pfizer_cpa_csv)
    val astellasCpaDF = CSV2DF(astellas_cpa_csv)
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

    val nhwaERD = nhwaCpaCvs.toERD(
        Map(
            "cpaDF" -> nhwaCpaDF,
            "hospDF" -> hospDIS,
            "prodDF" -> prodDIS,
            "phaDF" -> phaDF
        )
    )("cpaERD")
    val nhwaMinus = nhwaCpaDF.count() - nhwaERD.count()
    phDebugLog("nhwaERD count = " + nhwaERD.count())
    assert(nhwaMinus == 0, "nhwa: 转换后的ERD比源数据减少`" + nhwaMinus + "`条记录")

    val pfizerERD = pfizerCpaCvs.toERD(
        Map(
            "cpaDF" -> pfizerCpaDF,
            "hospDF" -> hospDIS,
            "prodDF" -> prodDIS,
            "phaDF" -> phaDF
        )
    )("cpaERD")
    val pfizerMinus = pfizerCpaDF.count() - pfizerERD.count()
    phDebugLog("pfizerERD count = " + pfizerERD.count())
    assert(pfizerMinus == 0, "pfizer: 转换后的ERD比源数据减少`" + pfizerMinus + "`条记录")

    val astellasERD = astellasCpaCvs.toERD(
        Map(
            "cpaDF" -> astellasCpaDF,
            "hospDF" -> hospDIS,
            "prodDF" -> prodDIS,
            "phaDF" -> phaDF
        )
    )("cpaERD")
    val astellasMinus = astellasCpaDF.count() - astellasERD.count()
    phDebugLog("astellasERD count = " + astellasERD.count())
    assert(astellasMinus == 0, "astellas: 转换后的ERD比源数据减少`" + astellasMinus + "`条记录")
    astellasERD.show(true)

    val astellasDIS = astellasCpaCvs.toDIS(
        Map(
            "cpaERD" -> astellasERD,
            "hospERD" -> hospDIS,
            "prodERD" -> prodDIS,
            "phaERD" -> phaDF
        )
    )("cpaDIS")
    val astellasDISMinus = astellasCpaDF.count() - astellasDIS.count()
    phDebugLog("astellasDIS count = " + astellasDIS.count())
    assert(astellasMinus == 0, "astellas: 转换后的DIS比源数据减少`" + astellasMinus + "`条记录")
    astellasDIS.show(true)

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