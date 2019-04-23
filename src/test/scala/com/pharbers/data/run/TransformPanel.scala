//package com.pharbers.run
//
//import com.pharbers.data.conversion._
//import com.pharbers.data.util._
//import com.pharbers.data.util.ParquetLocation._
//import com.pharbers.util.log.phLogTrait.phDebugLog
//
//object TransformPanel extends App {
//    import org.apache.spark.sql.functions._
//    import com.pharbers.data.util.sparkDriver.ss.implicits._
//
//    val pfizer_source_id = "5ca069e2eeefcc012918ec73"
//    val pfizer_cns_r_panel_csv = "/test/dcs/201804_CNS_R_panel_result.csv"
//
//    val panelConversion = PanelConversion(pfizer_source_id)
//    val hospConversion = HospConversion()
//    val prodConversion = ProdConversion()
//
//    val panelDF = CSV2DF(pfizer_cns_r_panel_csv)
//    val phaDF = Parquet2DF(HOSP_PHA_LOCATION)
//    val hospDIS = hospConversion.toDIS(Map(
//        "hospBaseERD" -> Parquet2DF(HOSP_BASE_LOCATION),
//        "hospAddressERD" -> Parquet2DF(HOSP_ADDRESS_BASE_LOCATION),
//        "hospPrefectureERD" -> Parquet2DF(HOSP_ADDRESS_PREFECTURE_LOCATION),
//        "hospCityERD" -> Parquet2DF(HOSP_ADDRESS_CITY_LOCATION),
//        "hospProvinceERD" -> Parquet2DF(HOSP_ADDRESS_PROVINCE_LOCATION)
//    ))("hospDIS")
//    val panelERDResult = panelConversion.toERD(
//        Map(
//            "panelDF" -> panelDF,
//            "hospDF" ->hospDIS,
//            "phaDF" -> phaDF,
//            "sourceDF" -> List.empty[(String, String, String)].toDF("_id", "COMPANY_ID", "MARKET")
//        )
//    )
//
//    val panelERDCount = panelERDResult("panelERD").count()
//    val panelDFCount = panelDF.count()
//    val sourceCount = panelERDResult("sourceERD").count()
//    phDebugLog(s"panelERDCount = $panelERDCount;panelDFCount = $panelDFCount; sourceCount = $sourceCount")
//
//    val prodDIS = prodConversion.toDIS(
//        Map(
//            "prodBaseERD" -> Parquet2DF(PROD_BASE_LOCATION),
//            "prodDeliveryERD" -> Parquet2DF(PROD_DELIVERY_LOCATION),
//            "prodDosageERD" -> Parquet2DF(PROD_DOSAGE_LOCATION),
//            "prodMoleERD" -> Parquet2DF(PROD_MOLE_LOCATION),
//            "prodPackageERD" -> Parquet2DF(PROD_PACKAGE_LOCATION),
//            "prodCorpERD" -> Parquet2DF(PROD_CORP_LOCATION)
//        )
//    )("prodDIS")
//
//    val panelDIS = panelConversion.toDIS(
//        Map(
//            "panelERD" -> panelERDResult("panelERD"),
//            "prodDIS" -> prodDIS,
//            "sourceERD" -> panelERDResult("sourceERD"),
//            "hospDIS" -> hospDIS
//        )
//    )("panelDIS")
//
//    val panelDISCount = panelDIS.count()
//    phDebugLog(s"panelERDCount = $panelERDCount;panelDISCount = $panelDISCount")
//}
