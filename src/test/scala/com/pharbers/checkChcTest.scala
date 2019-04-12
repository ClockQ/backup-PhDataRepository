package com.pharbers

import com.pharbers.common.{phFactory, savePath2Mongo}
import com.pharbers.spark.util.readParquet

object checkChcTest extends App {
	val driver = phFactory.getSparkInstance()

	import driver.conn_instance

	driver.sc.addJar("target/pharbers-data-repository-1.0-SNAPSHOT.jar")

	val path = "/test/chc/"
	val fileList = List("chc-table", "chc-city", "chc-date", "chc-manufacture", "chc-mole", "chc-oad", "chc-pack", "chc-product", "chc-revenue")
	def getNum(filePath: String): String ={
		val df = driver.setUtil(readParquet()).readParquet(filePath)
		filePath + ": "+ df.count()
	}

	val resultList = fileList.map(x => getNum(path + x))
	resultList.foreach(println(_))
	new savePath2Mongo().saveDF("/test/chc")
}
