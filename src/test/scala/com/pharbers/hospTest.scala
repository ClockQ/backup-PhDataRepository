package com.pharbers

import com.pharbers.common.phFactory
import com.pharbers.phDataConversion.{phDataHandFunc, phHospData, phRegionData}

object hospTest extends App {
	val driver = phFactory.getSparkInstance()

	import driver.conn_instance

	driver.sc.addJar("target/pharbers-data-repository-1.0-SNAPSHOT.jar")
	var df = driver.ss.read.format("com.databricks.spark.csv")
		.option("header", "true")
		.option("delimiter", ",")
		.load("/test/2019年Universe更新维护1.0.csv")
		.withColumn("addressId", phDataHandFunc.setIdCol())
		.cache()

	df.columns.foreach(x => {
		df = df.withColumnRenamed(x, x.trim)
	})

	new phHospData().getHospDataFromCsv(df)
	new phRegionData().getRegionDataFromCsv(df)
}
