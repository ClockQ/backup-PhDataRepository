package com.pharbers

import com.pharbers.data.util.getSparkDriver
import com.pharbers.phDataConversion.phCHCData
import com.pharbers.spark.util.readCsv

object phChcTest extends App {
	val driver = getSparkDriver()

	import driver.conn_instance

	val df = driver.setUtil(readCsv()).readCsv("/test/OAD CHC data for 5 cities to 2018Q3 v3.csv")
	new phCHCData().getDataFromDF(df)
}
