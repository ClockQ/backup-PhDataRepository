package com.pharbers.data.conversion.hosp

import com.pharbers.spark.phSparkDriver

/**
  * @description:
  * @author: clock
  * @date: 2019-04-24 10:22
  */
object phFactory{
    private lazy val sparkDriver: phSparkDriver = phSparkDriver("cui-test")

    def getSparkInstance(): phSparkDriver = {
        sparkDriver.sc.addJar("hdfs:///jars/phDataRepository/spark_driver-1.0.jar")
        sparkDriver.sc.addJar("hdfs:///jars/phDataRepository/mongo-java-driver-3.8.0.jar")
        sparkDriver
    }

}
