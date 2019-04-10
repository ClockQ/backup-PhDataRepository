package com.pharbers

import com.pharbers.common.{phFactory, savePath2Mongo}

object saveHospTest extends App {
	val driver = phFactory.getSparkInstance()

	import driver.conn_instance

	driver.sc.addJar("target/pharbers-data-repository-1.0-SNAPSHOT.jar")
	new savePath2Mongo().saveDF("/test/hosp")
}
