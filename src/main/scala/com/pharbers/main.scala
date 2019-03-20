package com.pharbers

import com.pharbers.common.phFactory
import com.pharbers.phDataConversion.phRegionData

object main extends App {
	val driver = phFactory.getSparkInstance()
	driver.sc.addJar("D:\\code\\pharbers\\phDataRepository new\\target\\pharbers-data-repository-1.0-SNAPSHOT.jar")
	driver.sc.addJar("C:\\Users\\EDZ\\.m2\\repository\\com\\pharbers\\spark_driver\\1.0\\spark_driver-1.0.jar")
	val df = driver.ss.read.format("com.databricks.spark.csv")
		.option("header", "true")
		.option("delimiter", ",")
		.load("/test/2019年Universe更新维护1.0.csv")
}
