package com.pharbers.data.job.AggregationJob

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

package object util {
    implicit class window(df: DataFrame){
        def yearGR(order: String, colName: String, partition: Column*): DataFrame ={
            val windowYearOnYear = Window.partitionBy(partition.head).orderBy(order).rangeBetween(-100, -100)
            df.withColumn(colName + "_YEAR_ON_YEAR", first(col(colName)).over(windowYearOnYear))
                    .withColumn(colName + "_YEAR_GROWTH", (col(colName + "_YEAR_ON_YEAR") - col(colName)) / col(colName + "_YEAR_ON_YEAR"))
                    .drop("YEAR_ON_YEAR")
        }

        def ringGR(order: String, colName: String, partition: Column*): DataFrame ={
            val windowYearOnYear = Window.partitionBy(partition: _*).orderBy(order).rangeBetween(-89, -1)
            df.withColumn(colName + "_RING", last(col(colName)).over(windowYearOnYear))
                    .withColumn(colName + "_RING_GROWTH", (col(colName + "_RING") - col(colName)) / col(colName + "_RING"))
                    .drop("RING")
        }

        def som(order: String, colName: String, partition: Column*): DataFrame ={
            val window = Window.partitionBy(partition: _*).rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
            df.withColumn(colName + "_SUM", sum(col(colName)).over(window))
                    .withColumn(colName + "_SOM", col(colName)/ col(colName + "_SUM"))
                    .drop(colName + "_SUM")
        }

        def addRank(order: String, partition: Column*): DataFrame = {
            val window = Window.partitionBy(partition: _*).orderBy(col(order).desc)
            df.withColumn(order + "_RANK", rank().over(window))
        }
    }

}
