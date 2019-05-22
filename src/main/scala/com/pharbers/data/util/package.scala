package com.pharbers.data

import com.pharbers.spark.util._
import org.apache.spark.sql.functions._
import com.pharbers.spark.phSparkDriver
import com.pharbers.data.model.oidSchema
import com.pharbers.util.log.phLogTrait.phDebugLog
import org.apache.spark.sql.{Column, DataFrame, SaveMode}

/**
  * @description: data util collection
  * @author: clock
  * @date: 2019-03-28 15:49
  */
package object util {

    implicit class SaveParquet(df: DataFrame) {
        def save2Parquet(location: String): DataFrame = {
            val name = location.split("/").last
            val path = location.split("/").init.mkString("", "/", "/")
            phDebugLog(s"save `$name` to `$path` parquet")
            df.write.mode(SaveMode.Append)
                    .option("header", value = true)
                    .parquet(location)
            df
        }
    }

    implicit class SaveMongo(df: DataFrame) {

        import org.apache.spark.sql.expressions.UserDefinedFunction

        def save2Mongo(name: String, conn_name: String = PhMongoConf.conn_name)(implicit sparkDriver: phSparkDriver): DataFrame = {
            phDebugLog(s"save `$name` to Mongo")
            sparkDriver.setUtil(com.pharbers.spark.util.save2Mongo()(sparkDriver.conn_instance))
                    .save2Mongo(
                        df.trimOId,
                        PhMongoConf.server_host,
                        PhMongoConf.server_port.toString,
                        conn_name,
                        name
                    )
            df
        }

        def trimOId: DataFrame = {
            //            phDebugLog(s"trim `ObjectID` in DataFrame")
            val trimOIdUdf: UserDefinedFunction = udf(oidSchema)
            if (df.columns.contains("_id")) df.withColumn("_id", trimOIdUdf(col("_id")))
            else df
        }

        def trimId: DataFrame = {
            if (df.columns.contains("_id")) df.withColumn("_id", lit(col("_id")("oid")))
            else df
        }
    }

    implicit class DFUtil(df: DataFrame) {

        def addColumn(colName: String, colValue: Any = null): DataFrame = {
            if (df.columns.contains(colName))
                df.withColumn(colName, when(col(colName).isNull, colValue).otherwise(col(colName)))
            else df.withColumn(colName, lit(colValue))
        }

        def generateId: DataFrame = {
            if (df.columns.contains("_id")) df
            else df.withColumn("_id", commonUDF.generateIdUdf()).cache()
        }

        def distinctByKey(keys: String*)(chooseBy: String = "", chooseFun: Column => Column = min): DataFrame = {
            val columns = df.columns
            val sortBy = if (chooseBy == "") columns.head else chooseBy

            df.groupBy(keys.head, keys.tail: _*)
                    .agg(chooseFun(struct(sortBy, columns.filter(_ != sortBy): _*)) as "tmp")
                    .select("tmp.*")
                    .select(columns.head, columns.tail: _*)
        }

        def str2Time: DataFrame = {
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
            if (df.columns.contains("YM")) df
            else df.withColumn("YM", commonUDF.time2StrUdf(col("TIME")))
        }

        def alignAt(alignDF: DataFrame): DataFrame = {
            alignDF.columns.foldRight(df)((a, b) => b.addColumn(a))
        }

        def addMonth(): DataFrame = {
            if (df.columns.contains("MONTH")) df
            else df.withColumn("MONTH", commonUDF.ym2MonthUdf(col("YM")))
        }
    }

    def FILE2DF(file_path: String, delimiter: String)(implicit sparkDriver: phSparkDriver): DataFrame =
        sparkDriver.setUtil(readCsv()(sparkDriver.conn_instance))
                .readCsv(file_path, delimiter, header = true).na.fill("")

    def CSV2DF(file_path: String)(implicit sparkDriver: phSparkDriver): DataFrame =
        FILE2DF(file_path, ",")

    def TXT2DF(file_path: String)(implicit sparkDriver: phSparkDriver): DataFrame =
        FILE2DF(file_path, "|")

    def Mongo2DF(collName: String, conn_name: String = PhMongoConf.conn_name)(implicit sparkDriver: phSparkDriver): DataFrame =
        sparkDriver.setUtil(readMongo()(sparkDriver.conn_instance)).readMongo(
            PhMongoConf.server_host,
            PhMongoConf.server_port.toString,
            conn_name,
            collName
        ).trimId

    def Parquet2DF(file_path: String)(implicit sparkDriver: phSparkDriver): DataFrame =
        sparkDriver.setUtil(readParquet()(sparkDriver.conn_instance)).readParquet(file_path)
}
