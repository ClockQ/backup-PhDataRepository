package com.pharbers.data.job.aggregationJob.functions

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object CompareFunction {
    def ringGRCompare(colName: String)(df: DataFrame): DataFrame = {
        import com.pharbers.data.util.PhWindowUtil._
        df.addRingGR("X_GROUP", colName, col("Y_GROUP"))
    }

    def yearGRCompare(colName: String)(df: DataFrame): DataFrame = {
        import com.pharbers.data.util.PhWindowUtil._
        df.addYearGR("X_GROUP", colName, col("Y_GROUP"))
    }

    def somCompare(colName: String)(df: DataFrame): DataFrame = {
        import com.pharbers.data.util.PhWindowUtil._
        df.addSom(colName, col("X_GROUP"), col("Y_GROUP"))
    }

    def rankCompare(colName: String)(df: DataFrame): DataFrame = {
        import com.pharbers.data.util.PhWindowUtil._
        df.addRank(colName, col("X_GROUP"), col("Y_GROUP"))
    }

//    def rankBySalesRingGR(df: DataFrame): DataFrame = {
//        import com.pharbers.data.util.PhWindowUtil._
//        df.addRank("SALES_RING_GROWTH", col("X_GROUP"), col("Y_GROUP"))
//    }
//
//    def ringGRByProductCount(df: DataFrame): DataFrame = {
//        import com.pharbers.data.util.PhWindowUtil._
//        df.addRingGR("DATE_GROUP", "PRODUCT_COUNT", col("PRODUCT_GROUP"))
//    }
//
//    def yearGRByProductCount(df: DataFrame): DataFrame = {
//        import com.pharbers.data.util.PhWindowUtil._
//        df.addYearGR("DATE_GROUP", "PRODUCT_COUNT", col("PRODUCT_GROUP"))
//    }
//
//    def ringGRBySalesSom(df: DataFrame): DataFrame = {
//        import com.pharbers.data.util.PhWindowUtil._
//        df.addRingGR("DATE_GROUP", "SALES_SOM", col("PRODUCT_GROUP"))
//    }
//
//    def yearGRBySalesSom(df: DataFrame): DataFrame = {
//        import com.pharbers.data.util.PhWindowUtil._
//        df.addYearGR("DATE_GROUP", "SALES_SOM", col("PRODUCT_GROUP"))
//    }
}
