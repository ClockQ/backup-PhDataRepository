package com.pharbers.data.job.AggregationJob.util

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object util {
    implicit class window(df: DataFrame){
        def yearGR(partition: String, order: String, colName: String, otherPartition: Column*): DataFrame ={
            val windowYearOnYear = Window.partitionBy(otherPartition :+ col(partition): _*).orderBy(order).rangeBetween(100, -100)
            df.withColumn("YEAR_ON_YEAR", first(col(colName).over(windowYearOnYear)))
                    .withColumn(colName + "_YEAR_GROWTH", (col("YEAR_ON_YEAR") - col(colName)) / col("YEAR_ON_YEAR"))
        }

        def ringGR(partition: String, order: String, colName: String, otherPartition: Column*): DataFrame ={
            val windowYearOnYear = Window.partitionBy(otherPartition :+ col(partition): _*).orderBy(order).rangeBetween(89, -1)
            df.withColumn("RING", first(col(colName).over(windowYearOnYear)))
                    .withColumn(colName + "_RING_GROWTH", (col("RING") - col(colName)) / col("RING"))
        }

        def som(partition: String, order: String, colName: String, otherPartition: Column*): DataFrame ={
            val window = Window.partitionBy(otherPartition :+ col(partition): _*).orderBy(order).rangeBetween(Window.unboundedPreceding, Window.unboundedFollowing)
            df.withColumn(colName + "_SUM", sum(col(colName).over(window)))
                    .withColumn(colName + "_SOM", col(colName)/ col(colName + "_SUM"))
        }

        def addRank(partition: String, order: String, otherPartition: Column*): DataFrame = {
            val window = Window.partitionBy(otherPartition :+ col(partition): _*).orderBy(order).rangeBetween(Window.unboundedPreceding, Window.unboundedFollowing)
            df.withColumn(order + "_RANK", rank().over(window))
        }
    }

}
