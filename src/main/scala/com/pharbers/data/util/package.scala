package com.pharbers.data

import com.pharbers.spark.util._
import org.apache.spark.sql.functions._
import com.pharbers.spark.phSparkDriver
import org.apache.spark.sql.{DataFrame, SaveMode}
import com.pharbers.util.log.phLogTrait.phDebugLog
import com.pharbers.spark.session.spark_conn_instance

/**
  * @description: data util collection
  * @author: clock
  * @date: 2019-03-28 15:49
  */
package object util {
    implicit val sparkDriver: phSparkDriver = getSparkDriver()
    implicit val conn_instance: spark_conn_instance = sparkDriver.conn_instance

    implicit class SaveParquet(df: DataFrame) {
        def save2Parquet(location: String): Unit = {
            val name = location.split("/").last
            val path = location.split("/").init.mkString("", "/", "/")
            phDebugLog(s"save `$name` to `$path` parquet")
            df.write.mode(SaveMode.Append)
                    .option("header", value = true)
                    .parquet(location)
        }
    }

    implicit class SaveMongo(df: DataFrame) {
        import org.apache.spark.sql.expressions.UserDefinedFunction

        def save2Mongo(name: String): Unit = {
            phDebugLog(s"save `$name` to Mongo")
            sparkDriver.setUtil(dataFrame2Mongo())
                    .dataFrame2Mongo(df.trimOId, PhMongoConf.server_host, PhMongoConf.server_port.toString, PhMongoConf.conn_name, name)
        }

        def trimOId: DataFrame = {
            phDebugLog(s"trim `ObjectID` in DataFrame")
            val trimOIdUdf: UserDefinedFunction = udf(oidSchema)
            if (df.columns.contains("_id")) df.withColumn("_id", trimOIdUdf(col("_id")))
            else df
        }

        def trimId: DataFrame = {
            phDebugLog(s"trim `ID` in DataFrame")
            if (df.columns.contains("_id")) df.withColumn("_id", lit(col("_id")("oid")))
            else df
        }
    }

    implicit class DFUtil(df: DataFrame) {

        def trim(colName: String, colValue: Any = ""): DataFrame = {
//            phDebugLog(s"trim `$colName`&`$colValue` in DataFrame")
            if (df.columns.contains(colName))
                df.withColumn(colName, when(col(colName).isNull, colValue).otherwise(col(colName)))
            else df.withColumn(colName, lit(colValue))
        }

        def generateId: DataFrame = {
//            phDebugLog(s"generate `ID` in DataFrame")
            if (df.columns.contains("_id")) df
            else df.withColumn("_id", commonUDF.generateIdUdf())
        }

        def str2Time: DataFrame = {
//            phDebugLog(s"`YM` to `Timestamp` in DataFrame")
            if (df.columns.contains("YM"))
                df.withColumn("TIME", commonUDF.str2TimeUdf(col("YM")))
            else
                df.withColumn(
                    "MONTH",
                    when(col("MONTH").>=(10), col("MONTH")).otherwise(
                        concat(col("MONTH").*(0).cast("int"), col("MONTH"))
                    )
                ).withColumn(
                    "YM",
                    concat(col("YEAR"), col("MONTH"))
                ).withColumn("TIME", commonUDF.str2TimeUdf(col("YM")))
        }

        def time2ym: DataFrame = {
            //            phDebugLog(s"`YM` to `Timestamp` in DataFrame")
            if (df.columns.contains("YM")) df
            else
                df.withColumn("YM", commonUDF.time2StrUdf(col("TIME")))
        }

        def alignAt(alignDF: DataFrame): DataFrame = {
            alignDF.columns.foldRight(df)((a, b) => b.trim(a, null))
        }
    }

    val CSV2DF: String => DataFrame =
        sparkDriver.setUtil(csv2RDD()).csv2RDD(_, ",", header = true).na.fill("")

    val TXT2DF: String => DataFrame =
        sparkDriver.setUtil(csv2RDD()).csv2RDD(_, "|", header = true).na.fill("")

    val Mongo2DF: String => DataFrame =
        sparkDriver.setUtil(mongo2DF()).mongo2DF(
            PhMongoConf.server_host,
            PhMongoConf.server_port.toString,
            PhMongoConf.conn_name, _
        ).trimId

    val Parquet2DF: String => DataFrame =
        sparkDriver.setUtil(readParquet()).readParquet(_)
}
