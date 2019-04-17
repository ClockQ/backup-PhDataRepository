package com.pharbers.test

import com.pharbers.data.job.maxResultAggregationJob


object testMaxResultConversionJob extends App{
    import com.pharbers.reflect.PhReflect._


    val maxResultERDLocation: String = "/test/dcs/201801_201901_CNS_R_panel_result_test"
    val ym: String ="201801,201802,201803,201804,201805,201806,201807,201808,201809,201810,201811,201812,201901"
    val market: String = "CNS_R"
    val companyId: String = "5ca069e2eeefcc012918ec73"

    maxResultAggregationJob(Map(
        "max_result_erd_location" -> maxResultERDLocation,
        "market" -> market,
        "company" -> companyId,
        "ym" -> ym))().exec()

}
