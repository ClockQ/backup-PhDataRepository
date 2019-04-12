package com.pharbers

import com.pharbers.common.phFactory
import com.pharbers.phDataConversion.phCHCData
import com.pharbers.spark.util.readCsv

object phChcTest extends App {
	val driver = phFactory.getSparkInstance()

	import driver.conn_instance

	driver.sc.addJar("target/pharbers-data-repository-1.0-SNAPSHOT.jar")

	val df = driver.setUtil(readCsv()).readCsv("/test/OAD CHC data for 5 cities to 2018Q3 v3.csv")
	new phCHCData().getDataFromDF(df)
}
