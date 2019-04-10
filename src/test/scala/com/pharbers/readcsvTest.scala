package com.pharbers

import com.pharbers.common.phFactory
import com.pharbers.mongoOITest.phSparkDriver
import com.pharbers.spark.phSparkDriver
import com.pharbers.spark.util.readCsv

object readcsvTest extends App {
	lazy val sparkDriver: phSparkDriver = phFactory.getSparkInstance()

	import sparkDriver.ss.implicits._
	import sparkDriver.conn_instance
	val df = sparkDriver.setUtil(readCsv()).readCsv("/test/OAD CHC data for 5 cities to 2018Q3 v3.csv")
	df.count()
	df.show(false)
}
