//package com.pharbers.test
//
//import com.pharbers.data.job.maxResultAggregationJob
//
//
//object testMaxResultAggregationJob extends App{
//    import com.pharbers.reflect.PhReflect._
//
//
//    val maxResultERDLocation: String = "/repository/maxResult"
//    val ym: String ="201801,201812"
//    val companyId: String = "5ca069e2eeefcc012918ec73"
//
//    maxResultAggregationJob(Map(
//        "max_result_erd_location" -> maxResultERDLocation,
//        "company" -> companyId,
//        "ym" -> ym))().exec()
//
//}
