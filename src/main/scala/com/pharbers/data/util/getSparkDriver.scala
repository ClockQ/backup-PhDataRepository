package com.pharbers.data.util

import com.pharbers.spark.phSparkDriver
import com.pharbers.util.log.phLogTrait.phDebugLog

/**
  * @description:
  * @author: clock
  * @date: 2019-03-28 15:36
  */
object getSparkDriver {
    def apply(appName: String = "data-conversion"): phSparkDriver = {
        phDebugLog("start spark driver")
        val sparkDriver: phSparkDriver = phSparkDriver(appName)
        sparkDriver.sc.addJar("hdfs:///jars/phDataRepository/spark_driver-1.0.jar")
        sparkDriver.sc.addJar("hdfs:///jars/phDataRepository/mongo-java-driver-3.8.0.jar")
        sparkDriver.sc.addJar("hdfs:///jars/phDataRepository/pharbers-data-repository-1.0-SNAPSHOT.jar")
        sparkDriver.sc.setLogLevel("ERROR")
        sparkDriver
    }
}