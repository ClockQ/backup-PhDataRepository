package com.pharbers.data.aggregation.functions

import org.apache.spark.sql.{DataFrame, RelationalGroupedDataset}

object AggFunction {

    def AggByYearMonthAndMarket(aggExpr: RelationalGroupedDataset => DataFrame, groups: String*)(df: DataFrame): DataFrame = {
        aggExpr(df.groupBy(groups.head, groups.tail: _*))
    }

//    def AggBySalesRankTop5AndYearMonthAndMarket(df: DataFrame): DataFrame = {
//        df.withColumn("SALES_RANK", when(col("SALES_RANK") > 5, "others").otherwise(col("SALES_RANK")))
//                .groupBy("MARKET", "YM", "SALES_RANK")
//                .maxResultAggExpr
//    }
//
//    def AggBySalesRankTop10AndYearMonthAndMarket(df: DataFrame): DataFrame = {
//
//        df.withColumn("SALES_RANK", when(col("SALES_RANK") > 10, "others").otherwise(lit("top10")))
//                .groupBy("MARKET", "YM", "SALES_RANK")
//                .maxResultAggExpr
//    }

//    implicit class AggExpr(df: RelationalGroupedDataset){
//        def maxResultAggExpr: DataFrame = {
//            df.agg(expr("sum(PROVINCE_COUNT) as PROVINCE_COUNT"),
//                expr("sum(CITY_COUNT) as CITY_COUNT"),
//                expr("sum(SALES) as SALES"),
//                expr("sum(UNITS) as UNITS"),
//                expr("first(COMPANY_ID) as COMPANY_ID"),
//                expr("count(PRODUCT_NAME) as PRODUCT_NAME_COUNT"),
//                expr("count(MOLE_NAME) as MOLE_NAME_COUNT"),
//                expr("count(CORP_NAME) as CORP_NAME_COUNT"),
//                expr("count(PH_CORP_NAME) as PH_CORP_NAME_COUNT"),
//                expr("count(MIN_PRODUCT) as PRODUCT_COUNT")
//            )
//        }
//    }
}


