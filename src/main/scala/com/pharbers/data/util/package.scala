package com.pharbers.data

import com.pharbers.spark.util._
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
        def save2Mongo(name: String): Unit = {
            phDebugLog(s"save `$name` to Mongo")
            sparkDriver.setUtil(dataFrame2Mongo())
                    .dataFrame2Mongo(df.trimOId, PhMongoConf.server_host, PhMongoConf.server_port.toString, PhMongoConf.conn_name, name)
        }
    }

    implicit class DFUnit(df: DataFrame) {

        import org.bson.types.ObjectId
        import org.apache.spark.sql.functions.{col, lit, udf}
        import org.apache.spark.sql.expressions.UserDefinedFunction

        def trim(colName: String, colValue: Any = null): DataFrame = {
            phDebugLog(s"trim `$colName`&`$colValue` to DataFrame")
            if (df.columns.contains(colName)) df
            else df.withColumn(colName, lit(colValue))
        }

        def generateId: DataFrame = {
            phDebugLog(s"generate `ID` to DataFrame")
            val generateOidUdf: UserDefinedFunction = udf { () => ObjectId.get().toString }
            if (df.columns.contains("_id")) df
            else df.withColumn("_id", generateOidUdf())
        }

        def trimOId: DataFrame = {
            phDebugLog(s"trim `ObjectID` to DataFrame")
            val trimOIdUdf: UserDefinedFunction = udf(toOId)
            if (df.columns.contains("_id")) df.withColumn("_id", trimOIdUdf(col("_id")))
            else df
        }

        def trimId: DataFrame = {
            phDebugLog(s"trim `ID` to DataFrame")
            if (df.columns.contains("_id")) df.withColumn("_id", lit(col("_id")("oid")))
            else df
        }
    }

    val CSV2DF: String => DataFrame = sparkDriver.setUtil(csv2RDD()).csv2RDD(_, ",", header = true)
    val Mongo2DF: String => DataFrame = sparkDriver.setUtil(mongo2DF()).mongo2DF(PhMongoConf.server_host, PhMongoConf.server_port.toString, PhMongoConf.conn_name, _).trimId
    val Parquet2DF: String => DataFrame = sparkDriver.setUtil(readParquet()).readParquet(_)
}
