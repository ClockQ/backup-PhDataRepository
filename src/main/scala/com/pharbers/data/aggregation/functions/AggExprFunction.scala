package com.pharbers.data.aggregation.functions

import org.apache.spark.sql.{DataFrame, RelationalGroupedDataset}
import org.apache.spark.sql.functions._

object AggExprFunction {
    def maxResultAggByMarketAndYmExpr(relationalGroupedDataset: RelationalGroupedDataset): DataFrame = {
        relationalGroupedDataset.agg(expr("sum(PROVINCE_COUNT) as PROVINCE_COUNT"),
            expr("sum(CITY_COUNT) as CITY_COUNT"),
            expr("sum(SALES) as SALES"),
            expr("sum(UNITS) as UNITS"),
            expr("first(COMPANY_ID) as COMPANY_ID"),
            expr("count(PRODUCT_NAME) as PRODUCT_NAME_COUNT"),
            expr("count(MOLE_NAME) as MOLE_NAME_COUNT"),
            expr("count(CORP_NAME) as CORP_NAME_COUNT"),
            expr("count(PH_CORP_NAME) as PH_CORP_NAME_COUNT"),
            expr("first(YM) as YM"),
            expr("first(MARKET) as MARKET"),
            expr("count(MIN_PRODUCT) as PRODUCT_COUNT")
        )
    }

//    def maxResultAggAndSalesRankExpr(relationalGroupedDataset: RelationalGroupedDataset): DataFrame = {
//        relationalGroupedDataset.agg(expr("sum(PROVINCE_COUNT) as PROVINCE_COUNT"),
//            expr("sum(CITY_COUNT) as CITY_COUNT"),
//            expr("sum(SALES) as SALES"),
//            expr("sum(UNITS) as UNITS"),
//            expr("first(COMPANY_ID) as COMPANY_ID"),
//            expr("count(PRODUCT_NAME) as PRODUCT_NAME_COUNT"),
//            expr("count(MOLE_NAME) as MOLE_NAME_COUNT"),
//            expr("count(CORP_NAME) as CORP_NAME_COUNT"),
//            expr("count(PH_CORP_NAME) as PH_CORP_NAME_COUNT"),
//            expr("count(MIN_PRODUCT) as PRODUCT_COUNT"),
//            expr("first(YM) as YM"),
//            expr("first(MARKET) as MARKET")
//        )
//    }
}
