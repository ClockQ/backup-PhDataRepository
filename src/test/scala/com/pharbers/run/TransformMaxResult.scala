package com.pharbers.run

import com.pharbers.spark.phSparkDriver
import com.pharbers.spark.util.csv2RDD
import com.pharbers.util.log.phLogTrait.phDebugLog

object TransformMaxResult extends App {

    import com.pharbers.data.conversion._
    import com.pharbers.data.util.ParquetLocation._
    import com.pharbers.data.util._

    val sparkDriver: phSparkDriver = getSparkDriver()
    val pfizer_source_id = "5ca069e2eeefcc012918ec73"
    val pfizer_inf_csv = "/workData/Export/96ca55cb-1413-c9f7-6b0a-aad3c739a88e/5b028f95ed925c2c705b85ba-201901-INF.csv"

    val hospCvs = HospConversion()
    val prodCvs = ProdConversion()
    val pfizerInfMaxCvs = MaxResultConversion(pfizer_source_id)

//    val pfizerInfDF = CSV2DF(pfizer_inf_csv)
    val pfizerInfDF = sparkDriver.setUtil(csv2RDD()).csv2RDD(pfizer_inf_csv, 31.toChar.toString, header = true).na.fill("")

    println("pfizerInfDF.count = " + pfizerInfDF.count())

    val maxERD = pfizerInfMaxCvs.toERD(
        Map(
            "maxDF" -> pfizerInfDF
        )
    )("maxERD")
    val pfizerMinus = pfizerInfDF.count() - maxERD.count()
    phDebugLog("maxERD count = " + maxERD.count())
    assert(pfizerMinus == 0, "pfizer INF max result: 转换后的ERD比源数据减少`" + pfizerMinus + "`条记录")

    val hospDIS = hospCvs.toDIS(
        Map(
            "hospBaseERD" -> Parquet2DF(HOSP_BASE_LOCATION),
            "hospAddressERD" -> Parquet2DF(HOSP_ADDRESS_BASE_LOCATION),
            "hospPrefectureERD" -> Parquet2DF(HOSP_ADDRESS_PREFECTURE_LOCATION),
            "hospCityERD" -> Parquet2DF(HOSP_ADDRESS_CITY_LOCATION),
            "hospProvinceERD" -> Parquet2DF(HOSP_ADDRESS_PROVINCE_LOCATION)
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

    val maxDIS = pfizerInfMaxCvs.toDIS(
        Map(
            "maxERD" -> maxERD,
            "hospDIS" -> hospDIS,
            "prodDIS" -> prodDIS
        )
    )("maxDIS")
    val pfizerDISMinus = pfizerInfDF.count() - maxDIS.count()
    phDebugLog("maxDIS count = " + maxDIS.count())
    assert(pfizerDISMinus == 0, "pfizer: 转换后的DIS比源数据减少`" + pfizerDISMinus + "`条记录")
    maxDIS.show(true)

}
