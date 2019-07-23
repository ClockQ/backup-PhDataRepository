package com.pharbers.data.qitest

import com.pharbers.data.util.PhMongoConf
import com.pharbers.data.util.spark.sparkDriver
import org.apache.spark.sql.DataFrame
import com.pharbers.spark.phSparkDriver
import com.pharbers.spark.util.readMongo

/**
  * @description:
  * @author: clock
  * @date: 2019-05-22 08:48
  */
object util {
    private val server_host = "123.56.179.133"
    private val server_port = "5555"
    private val conn_name = "pharbers-max-repository"

    def OnlineMongo2DF(collName: String)(implicit sparkDriver: phSparkDriver): DataFrame = {
        import com.pharbers.data.util.SaveMongo
        sparkDriver.setUtil(readMongo()(sparkDriver.conn_instance))
                .readMongo(server_host, server_port, conn_name, collName).trimId
    }

    def OnlineSave2Mongo(df: DataFrame, collName: String)(implicit sparkDriver: phSparkDriver): DataFrame = {
        import com.pharbers.data.util.SaveMongo
        sparkDriver.setUtil(com.pharbers.spark.util.save2Mongo()(sparkDriver.conn_instance))
                .save2Mongo(df.trimOId, server_host, server_port, conn_name, collName)
        df
    }
}
