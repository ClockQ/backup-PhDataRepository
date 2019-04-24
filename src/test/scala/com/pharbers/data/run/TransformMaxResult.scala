package com.pharbers.run

import com.pharbers.util.log.phLogTrait.phDebugLog
import org.apache.spark.sql.DataFrame

import scala.io.Source

object TransformMaxResult extends App {

    import com.pharbers.data.conversion._
    import com.pharbers.data.util.ParquetLocation._
    import com.pharbers.data.util._

    val pfizer_source_id = "5ca069e2eeefcc012918ec73"
    val pfizer_inf_csv = "/test/dcs/201801_201901_CNS_R_panel_result_test.csv"
//
    val hospCvs = HospConversion()
    val PROD_DEV_CVS = ProductDevConversion()//(ProductImsConversion(), ProductEtcConversion())
    val pfizerInfMaxCvs = MaxResultConversion(pfizer_source_id)

    val pfizerInfDF = FILE2DF(pfizer_inf_csv, 31.toChar.toString)

    println("pfizerInfDF.count = " + pfizerInfDF.count())

    val maxToErdResult = pfizerInfMaxCvs.toERD(
        Map(
            "maxDF" -> pfizerInfDF
        )
    )
    val maxERD = maxToErdResult("maxERD")
    val sourceERD = maxToErdResult("sourceERD")
    val pfizerMinus = pfizerInfDF.count() - maxERD.count()
    phDebugLog("maxERD count = " + maxERD.count())
    assert(pfizerMinus == 0, "pfizer INF max result: 转换后的ERD比源数据减少`" + pfizerMinus + "`条记录")
    maxERD.save2Parquet("/repository/maxResult")
    val hospDIS = hospCvs.toDIS(
        Map(
            "hospBaseERD" -> Parquet2DF(HOSP_BASE_LOCATION),
            "hospAddressERD" -> Parquet2DF(HOSP_ADDRESS_BASE_LOCATION),
            "hospPrefectureERD" -> Parquet2DF(HOSP_ADDRESS_PREFECTURE_LOCATION),
            "hospCityERD" -> Parquet2DF(HOSP_ADDRESS_CITY_LOCATION),
            "hospProvinceERD" -> Parquet2DF(HOSP_ADDRESS_PROVINCE_LOCATION)
        )
    )("hospDIS")
    val PROD_DEV_DIS: DataFrame = ???
//    PROD_DEV_CVS.toDIS(
//        Map(
//            "productDevERD" -> Parquet2DF(PROD_DEV_LOCATION),
//            "productEtcERD" -> Parquet2DF(PROD_ETC_LOCATION + "/" + pfizer_source_id),
//            "productImsERD" -> Parquet2DF(PROD_IMS_LOCATION)
//        )
//    )("productDIS")

    val maxDIS = pfizerInfMaxCvs.toDIS(
        Map(
            "maxERD" -> maxERD,
            "sourceERD" -> sourceERD,
            "hospDIS" -> hospDIS,
            "prodDIS" -> PROD_DEV_DIS
        )
    )("maxDIS")
    val pfizerDISMinus = pfizerInfDF.count() - maxDIS.count()
    phDebugLog("maxDIS count = " + maxDIS.count())
    assert(pfizerDISMinus == 0, "pfizer: 转换后的DIS比源数据减少`" + pfizerDISMinus + "`条记录")
    maxDIS.show(true)

}
