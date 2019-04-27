import com.pharbers.data.job.aggregationJob.MaxResultAggregationForMonthJob
//package com.pharbers.data.test
//
//import com.pharbers.data.job.aggregationJob._
//
//object testMaxResultAggForMonthJob extends App {
//    import com.pharbers.reflect.PhReflect._
//
//
//    val maxResultERDLocation: String = "/repository/maxResult"
//    val ym: String ="201701,201812"
//    val companyId: String = "5ca069e2eeefcc012918ec73"
//    val months = "1,2,3,4,5,6,7,8,9,10,11,12"
//    MaxResultAggregationForMonthJob(
//        Map(
//            "max_result_erd_location" -> maxResultERDLocation,
//            "company" -> companyId,
//            "ym" -> ym,
//            "months" -> months
//        )
//    )().exec()
//}
object testMaxResultAggForMonthJob extends App {
    import com.pharbers.reflect.PhReflect._


    val maxResultERDLocation: String = "/test/dcs/maxResult_nhwa"
    val ym: String ="201701,201812"
    val companyId: String = "5ca069bceeefcc012918ec72"
    val months = "1,2,3,4,5,6,7,8,9,10,11,12"
    MaxResultAggregationForMonthJob(
        Map(
            "max_result_erd_location" -> maxResultERDLocation,
            "company" -> companyId,
            "ym" -> ym,
            "months" -> months,
            "aggPath" -> "/test/dcs/nhwa_agg"
        )
    )().exec()
}