package com.pharbers.data.aggregation

import org.apache.spark.sql.{Column, DataFrame, RelationalGroupedDataset}
import org.apache.spark.sql.functions._

package object functions {
    def groupByXYGroup(group: String*): DataFrame => DataFrame = {
        df => df.withColumn("X_GROUP", col(group.head))
                .withColumn("Y_GROUP", col(group.tail.head))
    }

    // agg functions
    def aggByGroups(aggExpr: RelationalGroupedDataset => DataFrame, groups: String*): DataFrame => DataFrame = {
        df => aggExpr(df.withColumn("agg", concat(groups.map(x => col(x)): _*)).groupBy("agg"))
    }

    //Compare functions
    def ringGRCompare(colName: String): DataFrame => DataFrame = {
        import com.pharbers.data.util.PhWindowUtil._
        df => df.addRingGR("X_GROUP", colName, col("Y_GROUP"))
    }

    def yearGRCompare(colName: String): DataFrame => DataFrame = {
        import com.pharbers.data.util.PhWindowUtil._
        df => df.addYearGR("X_GROUP", colName, col("Y_GROUP"))
    }

    def somCompare(colName: String): DataFrame => DataFrame = {
        import com.pharbers.data.util.PhWindowUtil._
        df => df.addSom(colName, col("X_GROUP"), col("Y_GROUP"))
    }

    def rankCompare(colName: String): DataFrame => DataFrame = {
        import com.pharbers.data.util.PhWindowUtil._
        df => df.addRank(colName, col("X_GROUP"), col("Y_GROUP"))
    }

    //Expr functions
    def maxResultAggByProductsAndYmsExpr: RelationalGroupedDataset => DataFrame = {
        relationalGroupedDataset => relationalGroupedDataset.agg(
            expr("sum(SALES) as SALES"),
            expr("sum(UNITS) as UNITS"),
            expr("first(COMPANY_ID) as COMPANY_ID"),
            expr("first(YM) as YM"),
            expr("first(MARKET) as MARKET"),
            expr("sum(PRODUCT_COUNT) as PRODUCT_COUNT")
        )
    }

    def maxResultAggByProductAndYmsExpr: RelationalGroupedDataset => DataFrame = {
        relationalGroupedDataset => relationalGroupedDataset.agg(
            expr("sum(SALES) as SALES"),
            expr("sum(UNITS) as UNITS"),
            expr("first(COMPANY_ID) as COMPANY_ID"),
            expr("first(YM) as YM"),
            expr("first(PRODUCT) as PRODUCT"),
            expr("first(MARKET) as MARKET"),
            expr("first(PRODUCT_NAME) as PRODUCT_NAME"),
            expr("first(PH_CORP_NAME) as PH_CORP_NAME"),
            expr("count(MIN_PRODUCT) as PRODUCT_COUNT")
        )
    }

    def maxResultAggBySalesRankOtherAndYmsExpr: RelationalGroupedDataset => DataFrame = {
        relationalGroupedDataset => relationalGroupedDataset.agg(
            expr("sum(SALES) as SALES"),
            expr("sum(UNITS) as UNITS"),
            expr("sum(SALES_SOM) as SALES_SOM"),
            expr("first(COMPANY_ID) as COMPANY_ID"),
            expr("first(YM) as YM"),
            expr("first(MARKET) as MARKET"),
            expr("first(SALES_RANK) as SALES_RANK"),
            expr("sum(PRODUCT_COUNT) as PRODUCT_COUNT")
        )
    }

    //pretreatment functions
    def filterOne(colName: String, value: String): DataFrame => DataFrame ={
        df => df.filter(col(colName) === value)
    }

    def distinguishRankTop5AndOther(rankName: String, columns: String*): DataFrame => DataFrame = {
        df => columns.foldLeft(df.withColumn(rankName, when(col(rankName) > 5, "others").otherwise(col(rankName))))((a, b) => {
            a.withColumn(b, when(col(rankName) === "others", "others").otherwise(col(b)))
        })
    }

    def findRankTop10(rankName: String): DataFrame => DataFrame = {
        df => df.withColumn(rankName, when(col(rankName) > 10, "others").otherwise("top10"))
    }

}
