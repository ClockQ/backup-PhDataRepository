package com.pharbers.data.test

import com.pharbers.data.aggregation.MaxResultAggregationForMonth
import com.pharbers.data.run.TransformMaxResult.{hospDIS, maxCvs, productDevERD}
import com.pharbers.pactions.actionbase.{DFArgs, MapArgs, StringArgs}
import com.pharbers.data.util.spark._
object testMaxResultAggForMonth extends App {
    import com.pharbers.reflect.PhReflect._
    import com.pharbers.data.util._
    import com.pharbers.data.conversion._
    import com.pharbers.data.util.ParquetLocation._

    val maxResultERDLocation: String = "/test/dcs/maxResult_nhwa"
    val ym: String ="201701,201812"
    val companyId: String = "5ca069bceeefcc012918ec72"
    val months = "1,2,3,4,5,6,7,8,9,10,11,12"

    val maxCvs = MaxResultConversion()

    val hospDIS = HospConversion().toDIS(MapArgs(Map(
        "hospBaseERD" -> DFArgs(Parquet2DF(HOSP_BASE_LOCATION))
        , "hospAddressERD" -> DFArgs(Parquet2DF(HOSP_ADDRESS_BASE_LOCATION))
        , "hospPrefectureERD" -> DFArgs(Parquet2DF(HOSP_ADDRESS_PREFECTURE_LOCATION))
        , "hospCityERD" -> DFArgs(Parquet2DF(HOSP_ADDRESS_CITY_LOCATION))
        , "hospProvinceERD" -> DFArgs(Parquet2DF(HOSP_ADDRESS_PROVINCE_LOCATION))
    ))).getAs[DFArgs]("hospDIS")
    val hospDISCount = hospDIS.count()

    val productDevERD = Parquet2DF(PROD_DEV_LOCATION)

    val maxERD = Parquet2DF(maxResultERDLocation)

    val maxDIS = maxCvs.toDIS(MapArgs(Map(
        "maxERD" -> DFArgs(maxERD)
        , "prodDIS" -> DFArgs(productDevERD)
        , "hospDIS" -> DFArgs(hospDIS)
    ))).getAs[DFArgs]("maxDIS")

    MaxResultAggregationForMonth(
        MapArgs(
            Map(
                "max_result_erd_location" -> StringArgs(maxResultERDLocation),
                "company" -> StringArgs(companyId),
                "ym" -> StringArgs(ym),
                "months" -> StringArgs(months),
                "aggPath" -> StringArgs("/test/dcs/nhwa_agg"),
                "maxDIS" -> DFArgs(maxDIS)
            )
        )
    )().exec()
}